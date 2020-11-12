## Processamento de dados utilizando PySpark

---

## 1. Considerações gerais
Como funciona a solução:
- O desenvolvimento inicial da solução para análise de dados foi feita utilziando ```Jupyter Noteboo``` (com PySpark).
- Após entendimento dos dados e validação dos métodos, o código foi modularizado em arquivos ```.py```.
- O sistema está encapsulado em container ```Docker```, para facilitar a reprodução do projeto.

---

## 2. Como utilizar
Para reproduzir a aplicação, utilize o ```Makefile```

```bash
  cd src      # diretório onde está o Makefile e a aplicação
  make setup  # build da imagem docker
  make run    # inicializa aplicação
```

### 3. Hierarquia de Projeto

O projeto foi inicialmente desenvolvido utilizando ```Jupyter Notebook```. Portanto, dentro do diretório ```/jupy``` temos os dois ```Jupyter Notebook``` utilizados para resolver os problemas propostos (Task-01.ipynb e Task-01.ipynb). Os resultados de ambas aplicações estão no diretório ```/jupy/result```. Os conjuntos de dados utilizados estão no diretório ```/jupy.dataset```.

Após o entendimento do problema, bem como o entendimento dos dados, a solução foi desenvolvida utilizando arquivos ```.py```. Dentro do diretório ```/src``` temos toda a solução desenvolvida.
```
├── information
│   └── desafio_luizalabs_-_de.pdf                                           <- Enunciado do desafio
├── jupy
│   ├── dataset
│   │   ├── clientes_pedidos.csv                                             <- Dados para a Task 02
│   │   └── wordcount.txt                                                    <- Dados para a Task 01
│   ├── result
│   │   ├── task1
│   │   │   ├── part-00000-6091c20a-5024-4d72-ac09-a28af8a9245c-c000.csv     <- Resultado da Task 01
│   │   │   └── _SUCCESS
│   │   └── task2
│   │       ├── part-00000-7683f5da-c719-4e04-be8a-b0eb99dccbf8-c000.csv     <- Resultado da Task 02
│   │       └── _SUCCESS
│   ├── Task-01.ipynb                                                        <- JuPy com a solução da Task 01
│   └── Task-02.ipynb                                                        <- JuPy com a solução da Task 01
├── README.md
└── src
    ├── app.py                                                               <- Código principal
    ├── container-result                                                     <- Volume compartilhado com o Container>
    │   ├── task1
    │   │   ├── part-00000-9a4b6435-9e54-451b-8af9-9ce47ccc01c9-c000.csv     <- Resultado quando executa o container
    │   │   └── _SUCCESS
    │   └── task2
    │       ├── part-00000-85e5e55a-4409-46e7-93a3-ac163952f0f4-c000.csv     <- Resultado quando executa o container
    │       └── _SUCCESS
    ├── dataset
    │   ├── clientes_pedidos.csv                                             <- Dados para a Task 02
    │   └── wordcount.txt                                                    <- Dados para a Task 02
    ├── environment
    │   ├── Dockerfile                                                       <- Dockerfile para executar o spark-submit
    │   └── download-files.sh                                                <- Script para fazer download dos dados
    ├── Makefile                                                             <- Makefile
    ├── requirements.txt                                                     <- Bibliotecas utilizadas
    ├── result
    │   ├── task1
    │   │   ├── part-00000-52cc214a-a6bc-4b50-b8ee-00b7abe0c502-c000.csv     <- Resultado da Task 01
    │   │   └── _SUCCESS
    │   └── task2
    │       ├── part-00000-32a69ef4-0b07-4421-a008-fb2a9cb2d804-c000.csv     <- Resultado da Task 01
    │       └── _SUCCESS
    ├── service
    │   ├── __init__.py
    │   ├── task01.py                                                        <- Métodos do PySpark para a Task 01
    │   └── task02.py                                                        <- Métodos do PySpark para a Task 02
    └── utils
        ├── __init__.py
        └── logging_utils.py                                                 <- Configuração dos logs
```