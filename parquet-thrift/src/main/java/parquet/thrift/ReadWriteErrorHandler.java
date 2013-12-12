package parquet.thrift;

public interface ReadWriteErrorHandler {
    // corrupted record: throw new SkippableException(error("Error while reading", buffer), e);
}
