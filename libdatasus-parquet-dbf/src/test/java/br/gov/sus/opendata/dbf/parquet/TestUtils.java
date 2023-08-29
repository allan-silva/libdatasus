package br.gov.sus.opendata.dbf.parquet;

public class TestUtils {
  public static String getResourcePath(String resourceName) {
    return TestUtils.class.getClassLoader().getResource(resourceName).getPath();
  }
}
