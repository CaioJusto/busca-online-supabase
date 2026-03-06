"""
Busca Online para Supabase - Scraper PROJUDI (TJGO)
Extrai dados completos de processos do PROJUDI e salva no Supabase.
"""

import threading
import queue
import os
import re
import io
import socket
import subprocess
import time
import logging
import traceback
import tempfile
import json
from datetime import datetime

from seleniumbase import SB
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from fake_useragent import UserAgent
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
PROJUDI_LOGIN_URL = "https://projudi.tjgo.jus.br/LogOn?PaginaAtual=-200"
PROJUDI_BUSCA_URL = "https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4"
PROJUDI_PROCESSO_INPUT_XPATH = '/html/body/div/form/div/fieldset/div[4]/input'

DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
try:
    ua = UserAgent()
    user_agent = ua.random
except Exception:
    user_agent = DEFAULT_USER_AGENT


class DriverRestartNeeded(Exception):
    pass


class BrowserDegradationDetected(Exception):
    pass


def porta_debug_livre():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def limpar_processos_chrome_orfaos(logger=None, aguardar_liberacao=True):
    try:
        for tentativa in range(3):
            subprocess.run(["pkill", "-9", "chrome"], capture_output=True, timeout=10)
            subprocess.run(["pkill", "-9", "chromium"], capture_output=True, timeout=10)
            subprocess.run(["pkill", "-9", "chromedriver"], capture_output=True, timeout=10)
            result = subprocess.run(["pgrep", "-f", "chrome"], capture_output=True, timeout=10)
            if result.returncode != 0:
                break
            time.sleep(2)
        if aguardar_liberacao:
            time.sleep(5)
    except Exception as e:
        if logger:
            logger.debug(f"Erro ao limpar processos Chrome: {e}")


def configurar_driver(sb):
    sb.driver.implicitly_wait(0)
    sb.driver.execute_cdp_cmd('Page.enable', {})
    sb.driver.execute_cdp_cmd('Network.enable', {})
    sb.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
    sb.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        '''
    })


class ProjudiScraper:
    def __init__(self):
        self.lock = threading.Lock()
        self.thread_local = threading.local()
        self.max_navegadores = int(os.environ.get("MAX_WORKERS", "2"))
        self.projudi_usuario = os.environ.get("PROJUDI_USUARIO", "07228313151")
        self.projudi_senha = os.environ.get("PROJUDI_SENHA", "Senhaprojudi24.")
        self.processos_concluidos = set()
        self.processos_falha = {}
        self.sessoes_browser_ativas = 0
        self.sessoes_browser_lock = threading.Lock()
        self.chrome_cleanup_lock = threading.Lock()
        self.setup_logging()
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    def setup_logging(self):
        self.logger = logging.getLogger('ProjudiScraper')
        self.logger.setLevel(logging.DEBUG)
        if self.logger.handlers:
            return
        file_handler = None
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'application.log')
        try:
            file_handler = logging.FileHandler(log_path, mode='w')
            file_handler.setLevel(logging.DEBUG)
        except Exception:
            try:
                file_handler = logging.FileHandler(os.path.join(tempfile.gettempdir(), 'application.log'), mode='w')
                file_handler.setLevel(logging.DEBUG)
            except Exception:
                file_handler = None
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        if file_handler:
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    # ─── Supabase I/O ───

    def fetch_processos_tjgo(self):
        """Busca todos os processos TJGO não-arquivados do Supabase."""
        result = self.supabase.table('processos') \
            .select('id, numero_cnj, tribunal, custom_data') \
            .eq('tribunal', 'TJGO') \
            .eq('archived', False) \
            .not_.is_('numero_cnj', 'null') \
            .execute()
        return result.data or []

    def save_to_supabase(self, processo_id, dados, is_update=True):
        """Salva dados extraídos do PROJUDI no Supabase.

        Args:
            processo_id: ID do processo no Supabase
            dados: Dados extraídos do PROJUDI
            is_update: Se True (default), só atualiza andamentos, synced_at,
                       usuario_ultima_mov e campos custom_data do PROJUDI.
                       Se False (primeira sync), atualiza tudo incluindo SQL columns.
        """
        # Fetch existing custom_data to merge
        existing = self.supabase.table('processos') \
            .select('custom_data') \
            .eq('id', processo_id) \
            .single() \
            .execute()
        existing_custom = (existing.data or {}).get('custom_data') or {}

        # Build updated custom_data - always update these PROJUDI fields
        updated_custom = {
            **existing_custom,
            'projudi_autor': dados.get('autor', ''),
            'projudi_reu': dados.get('reu', ''),
            'projudi_andamentos': dados.get('andamentos', []),
            'projudi_synced_at': datetime.now().isoformat(),
            'projudi_usuario_ultima_mov': dados.get('usuario_ultima_mov', ''),
        }

        # Always update PROJUDI-specific custom_data fields
        if dados.get('comarca'):
            updated_custom['projudi_comarca'] = dados['comarca']
        if dados.get('valor_causa'):
            updated_custom['projudi_valor_causa'] = dados['valor_causa']

        # New PROJUDI fields - always saved to custom_data with projudi_ prefix
        new_projudi_fields = {
            'area': 'projudi_area',
            'serventia': 'projudi_serventia',
            'classe': 'projudi_classe',
            'valor_condenacao': 'projudi_valor_condenacao',
            'processo_originario': 'projudi_processo_originario',
            'fase_processual': 'projudi_fase_processual',
            'segredo_justica': 'projudi_segredo_justica',
            'data_transito_julgado': 'projudi_data_transito_julgado',
            'status_projudi': 'projudi_status',
            'prioridade_projudi': 'projudi_prioridade',
            'efeito_suspensivo': 'projudi_efeito_suspensivo',
            'julgado_2grau': 'projudi_julgado_2grau',
            'custas': 'projudi_custas',
            'penhora_rosto': 'projudi_penhora_rosto',
        }
        for dados_key, custom_key in new_projudi_fields.items():
            value = dados.get(dados_key, '')
            if value:
                updated_custom[custom_key] = value

        # Core fields to update directly (SQL columns)
        update_payload = {
            'custom_data': updated_custom,
            'updated_at': datetime.now().isoformat(),
        }

        # Only update SQL-level columns on first sync (creation), not on updates
        if not is_update:
            if dados.get('assunto') and dados['assunto'] not in ('', 'Não encontrado (timeout)'):
                update_payload['assunto'] = dados['assunto']
            if dados.get('data_distribuicao'):
                update_payload['data_distribuicao'] = dados['data_distribuicao']

        self.supabase.table('processos') \
            .update(update_payload) \
            .eq('id', processo_id) \
            .execute()

        mode_label = "update" if is_update else "creation"
        self.logger.info(f"[SUPABASE] Processo {processo_id} atualizado ({mode_label}) com {len(dados.get('andamentos', []))} andamentos")

    # ─── Selenium helpers (preserved from original) ───

    def _is_driver_connection_error(self, message):
        msg_lower = message.lower()
        indicators = [
            "connection refused", "newconnectionerror", "maxretryerror",
            "remotedisconnected", "connectionreseterror", "no such window",
            "target window already closed", "unable to connect",
            "chrome not reachable", "session not created",
            "invalid session id", "no such session",
            "chrome failed to start", "cannot find chrome binary",
        ]
        return any(ind in msg_lower for ind in indicators)

    def _marcar_sessao_browser_ativa(self, worker_id):
        with self.sessoes_browser_lock:
            self.sessoes_browser_ativas += 1

    def _desmarcar_sessao_browser_ativa(self, worker_id):
        with self.sessoes_browser_lock:
            self.sessoes_browser_ativas = max(0, self.sessoes_browser_ativas - 1)

    def _limpar_processos_chrome_com_seguranca(self, worker_id):
        with self.chrome_cleanup_lock:
            with self.sessoes_browser_lock:
                if self.sessoes_browser_ativas > 1:
                    self.logger.info(f"[Worker {worker_id}] Outras sessões ativas, pulando limpeza global")
                    return
            limpar_processos_chrome_orfaos(self.logger)

    def esperar_condicao(self, driver, condition, timeout=0.3, tentativas=2):
        for t in range(tentativas):
            try:
                return WebDriverWait(driver, timeout).until(condition)
            except TimeoutException:
                if t == tentativas - 1:
                    raise
            except Exception as e:
                if self._is_driver_connection_error(str(e)):
                    raise DriverRestartNeeded(str(e))
                raise

    def extrair_elemento_com_timeout(self, wait, xpath, timeout=0.1):
        try:
            try:
                return WebDriverWait(wait._driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                ).text.strip()
            except:
                if 'div[4]' in xpath:
                    return WebDriverWait(wait._driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, xpath.replace('div[4]', 'div[3]')))
                    ).text.strip()
                elif 'div[3]' in xpath:
                    return WebDriverWait(wait._driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, xpath.replace('div[3]', 'div[4]')))
                    ).text.strip()
                else:
                    raise
        except TimeoutException:
            return ""
        except Exception as e:
            if self._is_driver_connection_error(str(e)):
                raise DriverRestartNeeded(str(e))
            return ""

    def obter_texto_primeiro_xpath(self, wait, xpaths, timeout=0.5):
        for xpath in xpaths:
            valor = self.extrair_elemento_com_timeout(wait, xpath, timeout)
            if valor and not valor.startswith("Não encontrado") and not valor.startswith("Erro"):
                return valor
        return ""

    # ─── Login ───

    def realizar_login_projudi(self, driver):
        if self._login_ja_realizado():
            return
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                self.logger.info(f"Login no PROJUDI... (tentativa {tentativa + 1}/{max_tentativas})")
                try:
                    driver.get(PROJUDI_LOGIN_URL)
                except Exception as e:
                    if self._is_driver_connection_error(str(e)):
                        raise DriverRestartNeeded(str(e))

                # Wait for page to load
                time.sleep(2)
                self.logger.info(f"URL após navegação: {driver.current_url}")
                self.logger.info(f"Title: {driver.title}")

                login_input = None
                for i in range(10):
                    try:
                        login_input = driver.find_element(By.XPATH, '//*[@id="login"]')
                        break
                    except NoSuchElementException:
                        if i < 9:
                            time.sleep(0.3)
                if not login_input:
                    # Log page source snippet for debugging
                    try:
                        page_src = driver.page_source[:500]
                        self.logger.warning(f"Página não contém #login. Source: {page_src}")
                    except:
                        pass
                    login_input = self.esperar_condicao(driver, EC.presence_of_element_located((By.XPATH, '//*[@id="login"]')), timeout=3, tentativas=3)
                login_input.clear()
                login_input.send_keys(self.projudi_usuario)

                senha_input = None
                try:
                    senha_input = driver.find_element(By.XPATH, '//*[@id="senha"]')
                except NoSuchElementException:
                    senha_input = self.esperar_condicao(driver, EC.presence_of_element_located((By.XPATH, '//*[@id="senha"]')), timeout=1.5, tentativas=3)
                senha_input.clear()
                senha_input.send_keys(self.projudi_senha)

                entrar_button = None
                try:
                    entrar_button = driver.find_element(By.XPATH, '//*[@id="formLogin"]/div[4]/input[1]')
                except NoSuchElementException:
                    entrar_button = self.esperar_condicao(driver, EC.element_to_be_clickable((By.XPATH, '//*[@id="formLogin"]/div[4]/input[1]')), timeout=1.5, tentativas=3)
                entrar_button.click()

                for _ in range(10):
                    if "LogOn" not in driver.current_url:
                        break
                    time.sleep(0.1)

                self._selecionar_perfil_usuario(driver)
                self._fechar_popups_projudi(driver)

                try:
                    driver.get(PROJUDI_BUSCA_URL)
                except Exception as e:
                    if self._is_driver_connection_error(str(e)):
                        raise DriverRestartNeeded(str(e))

                self._set_login_realizado(True)
                self.logger.info("Login PROJUDI OK.")
                return
            except DriverRestartNeeded:
                raise
            except Exception as e:
                self.logger.error(f"Falha login tentativa {tentativa + 1}: {e}")
                if self._is_driver_connection_error(str(e)):
                    raise DriverRestartNeeded(str(e))
                if tentativa < max_tentativas - 1:
                    time.sleep(5)
                else:
                    raise

    def _set_login_realizado(self, valor):
        self.thread_local.login_realizado = valor

    def _reset_login_realizado(self):
        self.thread_local.login_realizado = False

    def _login_ja_realizado(self):
        return getattr(self.thread_local, 'login_realizado', False)

    def _selecionar_perfil_usuario(self, driver):
        try:
            for xpath in [
                "//fieldset//a[contains(@href, 'SelecionarPerfil')]",
                "/html/body/div[3]/fieldset/label[1]/a",
                "/html/body/div[3]/fieldset/label[2]/a"
            ]:
                try:
                    link = self.esperar_condicao(driver, EC.element_to_be_clickable((By.XPATH, xpath)), timeout=1.2, tentativas=4)
                    link.click()
                    return
                except TimeoutException:
                    continue
        except Exception as e:
            self.logger.warning(f"Perfil não selecionado: {e}")

    def _fechar_popups_projudi(self, driver):
        possiveis_popups = [
            "//button[contains(text(), 'Fechar')]",
            "//button[contains(text(), 'OK')]",
            "//a[contains(text(), 'Fechar')]",
            "//input[@value='Fechar']",
            "//input[@value='OK']",
            "//button[contains(@class, 'close')]",
            "//*[contains(@class, 'modal')]//button",
        ]
        for xpath in possiveis_popups:
            try:
                el = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].click();", el)
                time.sleep(0.1)
            except:
                pass

    def alternar_para_detalhes_processo(self, driver):
        try:
            driver.switch_to.default_content()
        except:
            pass
        # Try switching to iframes
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                try:
                    driver.switch_to.default_content()
                    driver.switch_to.frame(iframe)
                    try:
                        driver.find_element(By.XPATH, '//fieldset//fieldset')
                        return
                    except:
                        pass
                except:
                    pass
            driver.switch_to.default_content()
        except:
            pass

    # ─── Core extraction ───

    def extrair_campo_por_label(self, driver, label_text):
        """Extract a field value by finding its label text in the page."""
        try:
            scripts = [
                f"""
                var labels = document.querySelectorAll('span, div, td, label');
                for (var i = 0; i < labels.length; i++) {{
                    if (labels[i].textContent.trim() === '{label_text}') {{
                        var next = labels[i].nextElementSibling;
                        if (next) return next.textContent.trim();
                    }}
                }}
                return '';
                """
            ]
            for script in scripts:
                result = driver.execute_script(script)
                if result:
                    return result
        except Exception as e:
            self.logger.debug(f"Erro ao extrair campo '{label_text}': {e}")
        return ''

    def extrair_todos_andamentos(self, driver, wait):
        """Extrai TODOS os andamentos da tabela de movimentações (não só o último)."""
        andamentos = []
        # Try different table XPATHs
        table_xpaths = [
            '/html/body/div[2]/form/div[1]/div/div[1]/table/tbody',
            '/html/body/div[4]/form/div[1]/div/div[1]/table/tbody',
            '/html/body/div[3]/form/div[1]/div/div[1]/table/tbody',
        ]

        tbody = None
        for xpath in table_xpaths:
            try:
                tbody = WebDriverWait(driver, 0.5).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                break
            except:
                continue

        if not tbody:
            self.logger.warning("Tabela de andamentos não encontrada")
            return andamentos

        try:
            rows = tbody.find_elements(By.TAG_NAME, 'tr')
            self.logger.info(f"Encontradas {len(rows)} linhas de andamentos")

            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) < 3:
                        continue

                    # Column layout: [numero, movimentacao, data, usuario]
                    numero = cells[0].text.strip() if len(cells) > 0 else ''
                    movimentacao = cells[1].text.strip() if len(cells) > 1 else ''
                    data_hora = cells[2].text.strip() if len(cells) > 2 else ''
                    usuario = cells[3].text.strip() if len(cells) > 3 else ''

                    if not movimentacao:
                        continue

                    # Parse date
                    data_iso = ''
                    if data_hora:
                        try:
                            # Format: "03/03/2026 18:01:42"
                            dt = datetime.strptime(data_hora.strip(), '%d/%m/%Y %H:%M:%S')
                            data_iso = dt.isoformat()
                        except:
                            try:
                                dt = datetime.strptime(data_hora.strip().split()[0], '%d/%m/%Y')
                                data_iso = dt.isoformat()
                            except:
                                data_iso = data_hora

                    andamentos.append({
                        'numero': numero,
                        'movimentacao': movimentacao,
                        'dataHora': data_iso,
                        'dataHoraOriginal': data_hora,
                        'usuario': usuario,
                    })
                except Exception as e:
                    self.logger.debug(f"Erro ao extrair linha de andamento: {e}")
                    continue

        except Exception as e:
            self.logger.warning(f"Erro ao iterar andamentos: {e}")

        return andamentos

    def extrair_dados_processo(self, driver, wait, numero_processo):
        """Extrai todos os dados de um processo do PROJUDI."""
        tentativa = 0
        max_tentativas = 5
        self.logger.info(f"[INICIANDO] Extração: {numero_processo}")

        while tentativa < max_tentativas:
            try:
                current_url = driver.current_url
                ja_na_pagina = "BuscaProcesso" in current_url or "PaginaAtual=4" in current_url

                if tentativa > 0 or not ja_na_pagina:
                    try:
                        driver.get(PROJUDI_BUSCA_URL)
                    except Exception:
                        pass
                driver.switch_to.default_content()

                # Find input field
                processo_input = None
                for i in range(8):
                    try:
                        processo_input = driver.find_element(By.XPATH, PROJUDI_PROCESSO_INPUT_XPATH)
                        break
                    except NoSuchElementException:
                        try:
                            processo_input = driver.find_element(By.XPATH, '//*[@id="ProcessoNumero"]')
                            break
                        except NoSuchElementException:
                            try:
                                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                                for iframe in iframes:
                                    driver.switch_to.default_content()
                                    driver.switch_to.frame(iframe)
                                    try:
                                        processo_input = driver.find_element(By.XPATH, PROJUDI_PROCESSO_INPUT_XPATH)
                                        break
                                    except:
                                        driver.switch_to.default_content()
                                if processo_input:
                                    break
                            except:
                                pass
                    if i < 7:
                        time.sleep(0.03)

                if not processo_input:
                    processo_input = self.esperar_condicao(
                        driver, EC.presence_of_element_located((By.XPATH, PROJUDI_PROCESSO_INPUT_XPATH)),
                        tentativas=2
                    )
                processo_input.clear()
                processo_input.send_keys(numero_processo)

                # Clear archived filter
                try:
                    filter_button = None
                    try:
                        filter_button = driver.find_element(By.XPATH, "/html/body/div/form/div/fieldset/div[5]/fieldset/div[1]/label/button[2]")
                    except NoSuchElementException:
                        try:
                            filter_button = driver.find_element(By.CSS_SELECTOR, "button[name='imaLimparProcessoStatus']")
                        except NoSuchElementException:
                            filter_button = self.esperar_condicao(driver, EC.element_to_be_clickable((By.XPATH, "/html/body/div/form/div/fieldset/div[5]/fieldset/div[1]/label/button[2]")), timeout=0.3, tentativas=2)
                    if filter_button:
                        driver.execute_script("arguments[0].click();", filter_button)
                except:
                    pass

                # Click search button
                botao_clicado = False
                estrategias = [
                    ('XPATH', '//input[@name="imgSubmeter"]'),
                    ('XPATH', '//input[@value="Buscar"]'),
                    ('CSS', '#divBotoesCentralizados > input[type=submit]:nth-child(1)'),
                    ('CSS', 'input[name="imgSubmeter"]'),
                    ('XPATH', '/html/body/div[3]/form/div/fieldset/div[5]/input[1]'),
                    ('XPATH', '/html/body/div[3]/form/div/fieldset/div[5]/input[2]'),
                    ('XPATH', '//input[@type="submit" and contains(@value, "Buscar")]'),
                    ('CSS', 'input[type="submit"][value*="Buscar"]'),
                    ('XPATH', '//input[@type="submit"]')
                ]

                for tipo, loc in estrategias:
                    try:
                        botao = None
                        try:
                            botao = driver.find_element(By.XPATH if tipo == 'XPATH' else By.CSS_SELECTOR, loc)
                        except NoSuchElementException:
                            botao = self.esperar_condicao(driver,
                                EC.element_to_be_clickable((By.XPATH if tipo == 'XPATH' else By.CSS_SELECTOR, loc)),
                                timeout=0.3, tentativas=2)

                        try:
                            botao.click()
                            botao_clicado = True
                        except:
                            try:
                                driver.execute_script("arguments[0].click();", botao)
                                botao_clicado = True
                            except:
                                try:
                                    driver.execute_script("AlterarValue('PaginaAtual','2'); VerificarCampos();")
                                    botao_clicado = True
                                except:
                                    pass

                        if botao_clicado:
                            break
                    except:
                        continue

                if not botao_clicado:
                    raise Exception("Botão de busca não encontrado")

                # Wait for process data
                self.alternar_para_detalhes_processo(driver)
                elemento_encontrado = False
                for xpath_dados in [
                    '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset',
                    '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset',
                    '/html/body/div[3]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset',
                    '//fieldset//fieldset//fieldset[1]'
                ]:
                    try:
                        self.esperar_condicao(driver,
                            EC.presence_of_element_located((By.XPATH, xpath_dados)),
                            timeout=0.3, tentativas=2)
                        elemento_encontrado = True
                        break
                    except:
                        continue

                if not elemento_encontrado:
                    raise Exception("Dados do processo não encontrados")

                # Extract static fields
                campos_xpaths = {
                    'autor': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset/span[1]',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset',
                        '/html/body/div[3]/form/div[1]/fieldset/fieldset/fieldset[1]/fieldset'
                    ],
                    'reu': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[2]/fieldset/span[1]',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[2]/fieldset',
                        '/html/body/div[3]/form/div[1]/fieldset/fieldset/fieldset[2]/fieldset'
                    ],
                    'comarca': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[3]/span[1]',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[3]/span[1]'
                    ],
                    'valor_causa': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[3]/span[4]',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[3]/span[4]'
                    ],
                    'assunto': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[3]/span[3]/table/tbody/tr/td',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[3]/span[3]/table/tbody/tr/td'
                    ],
                    'data_distribuicao': [
                        '/html/body/div[2]/form/div[1]/fieldset/fieldset/fieldset[3]/span[8]',
                        '/html/body/div[4]/form/div[1]/fieldset/fieldset/fieldset[3]/span[8]'
                    ],
                }

                dados = {}
                for campo, xpaths in campos_xpaths.items():
                    dados[campo] = self.obter_texto_primeiro_xpath(wait, xpaths, timeout=0.1)

                # Clean Autor
                if dados['autor']:
                    dados['autor'] = re.sub(r'Raça:.*', '', dados['autor'], flags=re.IGNORECASE).strip()
                    for word in ['Nome', 'Social']:
                        dados['autor'] = dados['autor'].replace(word, '')
                    names = [n.strip() for n in dados['autor'].split('\n') if n.strip()]
                    dados['autor'] = '\n'.join(list(dict.fromkeys(names)))

                # Clean Reu
                if dados['reu']:
                    for word in ['Nome', 'Raça', 'Social']:
                        dados['reu'] = dados['reu'].replace(word, '')
                    names = [n.strip() for n in dados['reu'].split('\n') if n.strip()]
                    dados['reu'] = '\n'.join(list(dict.fromkeys(names)))

                # Parse Data Distribuicao to ISO
                if dados['data_distribuicao']:
                    try:
                        date_str = dados['data_distribuicao'].split()[0]
                        dt = datetime.strptime(date_str, '%d/%m/%Y')
                        dados['data_distribuicao'] = dt.strftime('%Y-%m-%d')
                    except:
                        dados['data_distribuicao'] = ''

                # Clean remaining fields
                for campo in ['comarca', 'valor_causa', 'assunto']:
                    if isinstance(dados.get(campo), str):
                        dados[campo] = dados[campo].strip()

                # Extract additional fields from "Outras Informações" using label-based approach
                campos_label = {
                    'area': 'Área:',
                    'serventia': 'Serventia:',
                    'classe': 'Classe:',
                    'valor_condenacao': 'Valor da Condenação:',
                    'processo_originario': 'Processo Originário:',
                    'fase_processual': 'Fase Processual:',
                    'segredo_justica': 'Segredo de Justiça:',
                    'data_transito_julgado': 'Data do Trânsito em Julgado:',
                    'status_projudi': 'Status:',
                    'prioridade_projudi': 'Prioridade:',
                    'efeito_suspensivo': 'Efeito Suspensivo:',
                    'julgado_2grau': 'Julgado 2º Grau:',
                    'custas': 'Custas:',
                    'penhora_rosto': 'Penhora no Rosto:',
                }
                for campo_key, label_text in campos_label.items():
                    dados[campo_key] = self.extrair_campo_por_label(driver, label_text)

                # Extract ALL andamentos
                dados['andamentos'] = self.extrair_todos_andamentos(driver, wait)

                # Get last movement user from first andamento
                if dados['andamentos']:
                    dados['usuario_ultima_mov'] = dados['andamentos'][0].get('usuario', '')

                self.logger.info(f"[OK] {numero_processo}: {len(dados['andamentos'])} andamentos extraídos")
                return dados

            except DriverRestartNeeded:
                raise
            except Exception as e:
                if self._is_driver_connection_error(str(e)):
                    raise DriverRestartNeeded(str(e))
                tentativa += 1
                self.logger.warning(f"[ERRO] Tentativa {tentativa}/{max_tentativas} para {numero_processo}: {e}")
                if tentativa == max_tentativas:
                    self.logger.error(f"Falha total para {numero_processo}")
                    return None

    # ─── Worker ───

    def processar_lote(self, worker_id, fila, resultados):
        """Worker thread que processa processos da fila."""
        self._reset_login_realizado()
        self._marcar_sessao_browser_ativa(worker_id)
        falhas_consecutivas = 0

        try:
            porta = porta_debug_livre()
            with SB(uc=True, headless=True,
                     chromium_arg=f"--remote-debugging-port={porta},--no-sandbox,--disable-dev-shm-usage,--disable-gpu,--disable-extensions",
                     agent=user_agent) as sb:
                configurar_driver(sb)
                driver = sb.driver
                wait = WebDriverWait(driver, 2)
                self.realizar_login_projudi(driver)

                while not fila.empty():
                    try:
                        processo = fila.get_nowait()
                    except queue.Empty:
                        break

                    processo_id = processo['id']
                    numero_cnj = processo['numero_cnj']

                    if processo_id in self.processos_concluidos:
                        continue

                    try:
                        dados = self.extrair_dados_processo(driver, wait, numero_cnj)
                        if dados:
                            # Determine if this is an update (already synced) or first creation
                            existing_custom = (processo.get('custom_data') or {})
                            is_update = bool(existing_custom.get('projudi_synced_at'))
                            self.save_to_supabase(processo_id, dados, is_update=is_update)
                            with self.lock:
                                self.processos_concluidos.add(processo_id)
                                resultados.append({'id': processo_id, 'status': 'ok', 'andamentos': len(dados.get('andamentos', []))})
                            falhas_consecutivas = 0
                        else:
                            with self.lock:
                                self.processos_falha[processo_id] = 'Dados não encontrados'
                                resultados.append({'id': processo_id, 'status': 'not_found'})
                            falhas_consecutivas += 1
                    except DriverRestartNeeded as e:
                        self.logger.error(f"[Worker {worker_id}] Driver precisa restart: {e}")
                        with self.lock:
                            self.processos_falha[processo_id] = str(e)
                        fila.put(processo)  # Re-queue
                        break
                    except Exception as e:
                        self.logger.error(f"[Worker {worker_id}] Erro em {numero_cnj}: {e}")
                        with self.lock:
                            self.processos_falha[processo_id] = str(e)
                            resultados.append({'id': processo_id, 'status': 'error', 'error': str(e)})
                        falhas_consecutivas += 1

                    if falhas_consecutivas >= 8:
                        self.logger.warning(f"[Worker {worker_id}] 8 falhas consecutivas, reiniciando...")
                        break

                    time.sleep(0.5)  # Throttle between processes
        except Exception as e:
            self.logger.error(f"[Worker {worker_id}] Erro fatal: {e}")
            self.logger.debug(traceback.format_exc())
        finally:
            self._desmarcar_sessao_browser_ativa(worker_id)

    # ─── Main entry point ───

    def run(self, processo_ids=None):
        """
        Executa o scraping.
        processo_ids: lista opcional de IDs específicos. Se None, processa todos TJGO.
        """
        self.logger.info("=" * 60)
        self.logger.info("Busca Online para Supabase - Iniciando")
        self.logger.info("=" * 60)

        processos = self.fetch_processos_tjgo()

        if processo_ids:
            processos = [p for p in processos if p['id'] in processo_ids]

        if not processos:
            self.logger.info("Nenhum processo TJGO para processar.")
            return {'processed': 0, 'results': []}

        self.logger.info(f"Total de processos TJGO: {len(processos)}")

        # Build queue
        fila = queue.Queue()
        for p in processos:
            fila.put(p)

        resultados = []
        num_workers = min(self.max_navegadores, len(processos))
        threads = []

        for w in range(num_workers):
            t = threading.Thread(target=self.processar_lote, args=(w, fila, resultados))
            threads.append(t)
            t.start()
            if w < num_workers - 1:
                time.sleep(10)  # Stagger worker startup

        for t in threads:
            t.join()

        # Summary
        ok = sum(1 for r in resultados if r['status'] == 'ok')
        fail = sum(1 for r in resultados if r['status'] != 'ok')
        total_andamentos = sum(r.get('andamentos', 0) for r in resultados)
        self.logger.info("=" * 60)
        self.logger.info(f"Concluído: {ok} OK, {fail} falhas, {total_andamentos} andamentos totais")
        self.logger.info("=" * 60)

        return {'processed': ok, 'failed': fail, 'total_andamentos': total_andamentos, 'results': resultados}


# ─── HTTP server for triggering via API ───

def create_http_handler(scraper):
    """Creates a simple HTTP handler for triggering the scraper."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path == '/sync':
                content_length = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(content_length)) if content_length else {}
                processo_ids = body.get('processo_ids')

                # Run in thread to not block
                result = scraper.run(processo_ids=processo_ids)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(result).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def do_GET(self):
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'ok'}).encode())
            else:
                self.send_response(404)
                self.end_headers()

    return Handler


if __name__ == "__main__":
    import sys

    scraper = ProjudiScraper()

    if '--server' in sys.argv:
        port = int(os.environ.get("PORT", "8080"))
        Handler = create_http_handler(scraper)
        from http.server import HTTPServer
        server = HTTPServer(('0.0.0.0', port), Handler)
        scraper.logger.info(f"HTTP server rodando em :{port}")
        scraper.logger.info(f"  POST /sync - Executar sync")
        scraper.logger.info(f"  GET /health - Health check")
        server.serve_forever()
    else:
        # Run directly
        result = scraper.run()
        print(json.dumps(result, indent=2))
