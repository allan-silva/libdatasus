package br.gov.sus.opendata.dbf.parquet;

import java.nio.file.Path;
import java.util.Objects;

/**
 * {@link ConvertTask} provides configurations about DBC/DBF conversion process
 */
public class ConvertTask {
  private final Path input;

  private final Path output;

  private final String schemaName;

  private final boolean combine;

  private ConvertTask(Builder builder) {
    this.input = builder.input;
    this.output = builder.output;
    this.schemaName = builder.schemaName;
    this.combine = builder.combine;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConvertTask that = (ConvertTask) o;
    return input.equals(that.input)
        && output.equals(that.output)
        && schemaName.equals(that.schemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(input, output, schemaName);
  }

  /**
   * Input file or directory to be converted.
   * @return path to resource.
   */
  public Path getInput() {
    return input;
  }

  /**
   * Output file or directory containing the result .parquet file.
   * @return path to resource.
   */
  public Path getOutput() {
    return output;
  }

  /**
   * Schema name used to emit parquet message.
   * @return message name for parquet file.
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * When {@link ConvertTask#getInput()} is a directory, enabling this feature allows to combine multiple DBC/DFB file in one single .parquet file.
   * @return flag indication for combine multiple DBC/DBF file in a single parquet file.
   */
  public boolean combine() {
    return combine;
  }

  public static class Builder {
    Path input;

    Path output;

    String schemaName;

    boolean combine = false;

    /**
     * @see ConvertTask#getInput()
     */
    public Builder input(Path input) {
      this.input = input;
      return this;
    }

    /**
     * @see ConvertTask#getOutput()
     */
    public Builder output(Path output) {
      this.output = output;
      return this;
    }

    /**
     * @see ConvertTask#getSchemaName()
     */
    public Builder schemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    /**
     * @see ConvertTask#combine()
     */
    public Builder combine(boolean combine) {
      this.combine = combine;
      return this;
    }

    /**
     * @see ConvertTask#combine()
     */
    public Builder combineFiles() {
      this.combine = true;
      return this;
    }

    public ConvertTask build() {
      return new ConvertTask(this);
    }
  }
}
