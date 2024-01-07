package br.gov.sus.opendata.dbf.parquet;

import java.nio.file.Path;
import java.util.Objects;

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

  public Path getInput() {
    return input;
  }

  public Path getOutput() {
    return output;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public boolean combine() {
    return combine;
  }

  public static class Builder {
    Path input;

    Path output;

    String schemaName;

    boolean combine = false;

    boolean decompressDbc = true;

    public Builder input(Path input) {
      this.input = input;
      return this;
    }

    public Builder output(Path output) {
      this.output = output;
      return this;
    }

    public Builder schemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder combine(boolean combine) {
      this.combine = combine;
      return this;
    }

    public Builder combineFiles() {
      this.combine = true;
      return this;
    }

    public Builder decompressDbc(boolean decompressDbc) {
      this.decompressDbc = decompressDbc;
      return this;
    }

    public ConvertTask build() {
      return new ConvertTask(this);
    }
  }
}
