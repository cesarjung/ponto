name: importar_bd_geral
on:
  schedule: [{ cron: "0 10,12 * * *" }]  # 07:00 e 09:00 America/Sao_Paulo
  workflow_dispatch:
env:
  TZ: America/Sao_Paulo
  PYTHONUNBUFFERED: "1"
jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"
      - name: criar credenciais.json
        shell: bash
        run: |
          printf '%s' "${{ secrets.GOOGLE_CREDENTIALS }}" > credenciais.json
          python -c "import json; json.load(open('credenciais.json','r',encoding='utf-8')); print('OK credenciais.json')"
      - run: pip install -r requirements.txt
      - run: python Importar_BD_Geral.py
