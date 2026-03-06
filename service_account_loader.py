import os
import json

def _candidate_paths():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    paths = []
    if env_path:
        paths.append(env_path)
    paths.append(os.path.join(base_dir, 'service_account.json'))
    return paths

def load_service_account():
    """
    Verifica se existe o arquivo service_account.json. Se não existir,
    tenta carregar das variáveis de ambiente.
    """
    # Verifica se o arquivo service_account.json existe em caminhos provaveis
    for path in _candidate_paths():
        if os.path.exists(path):
            print(f"Arquivo service_account.json encontrado em: {path}")
            return True
    
    # Tenta carregar das variáveis de ambiente
    service_account_json = os.environ.get('SERVICE_ACCOUNT_JSON')
    if service_account_json:
        try:
            # Verifica se o conteúdo é um JSON válido
            json_content = json.loads(service_account_json)

            # Tenta salvar no caminho definido pelo env ou no diretório do projeto
            target_path = None
            for path in _candidate_paths():
                try:
                    dir_name = os.path.dirname(path) or '.'
                    os.makedirs(dir_name, exist_ok=True)
                    with open(path, 'w') as f:
                        json.dump(json_content, f)
                    target_path = path
                    break
                except Exception:
                    continue

            if target_path:
                print(f"Arquivo service_account.json criado a partir da variável de ambiente em: {target_path}")
                return True

            print("ERRO: Não foi possível salvar o service_account.json em nenhum caminho esperado")
            return False
        except json.JSONDecodeError:
            print("ERRO: O conteúdo da variável SERVICE_ACCOUNT_JSON não é um JSON válido")
            return False
    else:
        print("ERRO: Arquivo service_account.json não encontrado e variável de ambiente SERVICE_ACCOUNT_JSON não configurada")
        return False

if __name__ == "__main__":
    load_service_account() 
