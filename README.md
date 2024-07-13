# libdatasus
Biblioteca para conversão de arquivos DBF para o formato parquet.

[Esse projeto faz parte do TCC do curso de pós graduação em Engenharia de Dados da PUC Minas (2022).](https://github.com/allan-silva/DE-puc-tcc)

# Uso

## Descompressão de arquivos DBC

### Dependência
https://mvnrepository.com/artifact/br.dev.contrib.gov.sus.opendata/libdatasus-dbc  

```
        <dependency>
            <groupId>br.dev.contrib.gov.sus.opendata</groupId>
            <artifactId>libdatasus-dbc</artifactId>
            <version>1.0.7</version>
        </dependency>
```
### Descomprimindo um arquivo

```java
import br.gov.sus.opendata.dbc.DbcNativeDecompressor;

...

  Path dbcPath = Path.of("/tmp/dbc/CIHASP1608.dbc");
  DbcNativeDecompressor.DecompressStats decompressStats = DbcNativeDecompressor.decompress(dbcPath);
  
  System.out.printf("Input file: %s - Size %s\n", decompressStats.getInputFileName(), decompressStats.getInputFileSize());
  System.out.printf("Output file: %s - Size %s\n", decompressStats.getOutputFileName(), decompressStats.getOutputFileSize());
  System.out.printf("Decompress time: %s", decompressStats.getDecompressTime());
```
**Output:**  
```
Input file: /tmp/dbc/CIHASP1608.dbc - Size 11403808
Output file: /tmp/dbc/CIHASP1608.dbc.dbf - Size 85876736
Decompress time: 210

```
### Descomprimindo um arquivo para um destino específico
```java
  Path dbcPath = Path.of("/tmp/dbc/CIHASP1608.dbc");
  Path dbfDestinationPath = Path.of("/tmp/dbc/CIHASP1608-decomp.dbf");
  DbcNativeDecompressor.DecompressStats decompressStats = DbcNativeDecompressor.decompress(dbcPath, dbfDestinationPath);
  
  System.out.printf("Input file: %s - Size %s\n", decompressStats.getInputFileName(), decompressStats.getInputFileSize());
  System.out.printf("Output file: %s - Size %s\n", decompressStats.getOutputFileName(), decompressStats.getOutputFileSize());
  System.out.printf("Decompress time: %s", decompressStats.getDecompressTime());
```
**Output:**  
```
Input file: /tmp/dbc/CIHASP1608.dbc - Size 11403808
Output file: /tmp/dbc/CIHASP1608-decomp.dbf - Size 85876736
Decompress time: 207

```
### Descomprimindo uma lista de arquivos presentes em um diretório
```java
  Path dbcDirectoryPath = Path.of("/tmp/dbc/");
  List<DbcNativeDecompressor.DecompressStats> decompressStatsList =
          DbcNativeDecompressor.decompressFromDirectory(dbcDirectoryPath);

  for (var decompressStats : decompressStatsList) {
      System.out.println();
      System.out.printf("Input file: %s - Size %s\n", decompressStats.getInputFileName(), decompressStats.getInputFileSize());
      System.out.printf("Output file: %s - Size %s\n", decompressStats.getOutputFileName(), decompressStats.getOutputFileSize());
      System.out.printf("Decompress time: %s\n", decompressStats.getDecompressTime());
      System.out.println();
  }

  System.out.printf("Total time: %s\n",
          decompressStatsList
                  .stream()
                  .mapToLong(DbcNativeDecompressor.DecompressStats::getDecompressTime).sum());
  System.out.printf("Initial size: %s\n",
          decompressStatsList
                  .stream()
                  .mapToLong(DbcNativeDecompressor.DecompressStats::getInputFileSize).sum());
  System.out.printf("Decompress size: %s\n",
          decompressStatsList
                  .stream()
                  .mapToLong(DbcNativeDecompressor.DecompressStats::getOutputFileSize).sum());
```
**Output:**  
```
Input file: /tmp/dbc/CIHASP1608.dbc - Size 11403808
Output file: /tmp/dbc/CIHASP1608.dbc.dbf - Size 85876736
Decompress time: 209


Input file: /tmp/dbc/POBR2023.dbc - Size 6303998
Output file: /tmp/dbc/POBR2023.dbc.dbf - Size 23461888
Decompress time: 100

Total time: 309
Initial size: 17707806
Decompress size: 109338624

```


# Dependências de software relacionada com os formatos DBC/DBF

Descompressor de arquivos DBC baseado no trabalho de Mark Adler <madler@alumni.caltech.edu> (zlib), Daniela Petruzalek ([blast-dbf](https://github.com/eaglebh/blast-dbf)) e Pablo Fonseca ([blast-dbf](https://github.com/eaglebh/blast-dbf)).

Leitor de arquivos DBF utiliza a biblioteca [JavaDBF](https://github.com/albfernandez/javadbf) de Alberto Fernández

# Problemas conhecidos
https://github.com/allan-silva/libdatasus/issues
