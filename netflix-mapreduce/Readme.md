# Trabalho Prático: MapReduce no Catálogo da Netflix

Este repositório contém a implementação de um Job MapReduce desenvolvido em **Java 8** utilizando o framework **Apache Hadoop** para análise estatística descritiva do dataset de títulos da Netflix.

---

## 🛠️ Pré-requisitos e Arquitetura do Ambiente

Para mitigar incompatibilidades de caminhos com espaços no Windows (como `C:\Program Files`) e conflitos de versões do ecossistema, o ambiente foi padronizado da seguinte forma:
* **Java SDK:** Eclipse Adoptium JDK 8 (instalado em `C:\java8`)
* **Hadoop:** Apache Hadoop 3.x (instalado em `C:\hadoop`)
* **Build Tool:** Apache Maven

---

## 🚀 Guia de Execução

### 1. Compilação do Projeto (Mojo Maven)
Para gerar o artefato binário garantindo a compatibilidade de bytecode com o Java 8, execute o comando abaixo no terminal do projeto (VS Code/PowerShell):

```powershell
mvn clean package

--Para disparar o pipeline de Map, Shuffle e Reduce, execute o comando abaixo no PowerShell:

""java -cp "C:\TrabalhoHadoop\netflix-mapreduce-1.0-SNAPSHOT.jar;C:\hadoop\share\hadoop\common\*;C:\hadoop\share\hadoop\common\lib\*;C:\hadoop\share\hadoop\mapreduce\*;C:\hadoop\share\hadoop\yarn\*;C:\hadoop\share\hadoop\hdfs\*" NetflixAnalysis file:///C:/TrabalhoHadoop/netflix_titles.csv file:///C:/TrabalhoHadoop/resultado ""