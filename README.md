# Automação Sheets/Drive

## Rodar localmente
1. Python 3.11
2. `pip install -r requirements.txt`
3. Salve seu service account como `credenciais.json` na raiz (não commitar)
4. `python Importar_BD_Geral.py` ou `python ponto-geral.py`

## GitHub Actions
- Adicionar secret `GOOGLE_CREDENTIALS` com o **conteúdo JSON** do service account.
- Workflows recriam `credenciais.json` em runtime e executam os scripts nos horários agendados.
