# Database Synchronization Script
Este script conecta-se a dois bancos de dados e sincroniza registros específicos entre eles, atualizando os dados conforme necessário. Ele utiliza a biblioteca psycopg2 para conexões e operações nos bancos e dotenv para gerenciar as variáveis de ambiente, garantindo a segurança dos detalhes de conexão.

## Funcionalidades
Conexão simultânea a dois bancos de dados (Banco A e Banco B)
Transferência de dados entre os bancos utilizando a técnica ON CONFLICT DO UPDATE para inserir ou atualizar registros
Atualização automática do campo transferir para False após a sincronização bem-sucedida
Estrutura do Código
Conexão com os bancos: O script se conecta aos bancos de dados de origem (Banco A) e destino (Banco B) utilizando detalhes de conexão armazenados em variáveis de ambiente.

Definição de colunas e tabelas: As tabelas e suas colunas são definidas manualmente. Atualmente, o script inclui:

adm (colunas: nome, email, senha)
evento_analise (colunas: nome, descricao, dt_evento, organizador, status)
filtros (coluna: categoria)
Transferência de dados com atualização: Para cada tabela, o script:

Seleciona os registros onde o campo transferir está marcado como True
Insere ou atualiza os registros no banco de dados de destino
Atualiza o campo transferir para False após a sincronização para evitar duplicação
Fechamento das conexões: Os cursores e as conexões são fechados após a conclusão do processo de sincronização.


