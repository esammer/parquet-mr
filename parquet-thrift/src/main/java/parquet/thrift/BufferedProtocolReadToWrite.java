/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.MapType;
import parquet.thrift.struct.ThriftType.SetType;
import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftTypeID;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Class to read from one protocol in a buffer and then write to another one
 * When there is an exception during reading, it's a skippable exception.
 * When schema is not compatible, the {@link SkippableException} will be passed to {@link ReadWriteErrorHandler#handleSkippedCorruptedRecord(SkippableException)}.
 * <p/>
 * When there are fields in the data that are not defined in the schema, the fields will be ignored and the handler will
 * be notified through {@link ReadWriteErrorHandler#handleFieldIgnored(org.apache.thrift.protocol.TField)}
 * and {@link parquet.thrift.BufferedProtocolReadToWrite.ReadWriteErrorHandler#handleRecordHasFieldIgnored()}
 *
 * @author Julien Le Dem
 */
public class BufferedProtocolReadToWrite implements ProtocolPipe {

  private static final Action STRUCT_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeFieldStop();
      out.writeStructEnd();
    }

    @Override
    public String toDebugString() {
      return ")";
    }
  };
  private static final Action FIELD_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeFieldEnd();
    }

    @Override
    public String toDebugString() {
      return ";";
    }
  };
  private static final Action MAP_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeMapEnd();
    }

    @Override
    public String toDebugString() {
      return "]";
    }
  };
  private static final Action LIST_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeListEnd();
    }

    @Override
    public String toDebugString() {
      return "}";
    }
  };
  private static final Action SET_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeSetEnd();
    }

    @Override
    public String toDebugString() {
      return "*}";
    }
  };
  //error handlers are global
  private static ReadWriteErrorHandler errorHandler;
  private final StructType thriftType;

  public BufferedProtocolReadToWrite(StructType thriftType) {
    super();
    this.thriftType = thriftType;
  }

  public static void registerErrorHandler(ReadWriteErrorHandler handler) {
    errorHandler = handler;
  }

  public static void unregisterAllErrorHandlers() {
    errorHandler = null;
  }

  /**
   * reads one record from in and writes it to out
   * An Exception can be used to skip a bad record
   *
   * @param in  input protocol
   * @param out output protocol
   * @throws TException         when an error happened while writing. Those are usually not recoverable
   * @throws SkippableException when an error happened while reading. Ignoring those will skip the bad records.
   *                            reference {@link ReadWriteErrorHandler} to implement error handling procedure when SkippableException is thrown
   */
  @Override
  public void readOne(TProtocol in, TProtocol out) throws TException {
    List<Action> buffer = new LinkedList<Action>();
    try {
      boolean hasFieldsIgnored = readOneStruct(in, buffer, thriftType);
      if (hasFieldsIgnored) {
        notifyRecordHasFieldIgnored();
      }
    } catch (Exception e) {
      notifySkippedCorruptedRecord(new SkippableException(error("Error while reading", buffer), e));
      return;
    }
    try {
      for (Action a : buffer) {
        a.write(out);
      }
    } catch (Exception e) {
      throw new TException(error("Can not write record", buffer), e);
    }
  }

  private void notifyRecordHasFieldIgnored() {
    if (errorHandler != null) {
      errorHandler.handleRecordHasFieldIgnored();
    }
  }

  private void notifySkippedCorruptedRecord(SkippableException e) {
    if (errorHandler != null) {
      errorHandler.handleSkippedCorruptedRecord(e);
    }
  }

  private void notifyIgnoredFieldsOfRecord(TField field) {
    if (errorHandler != null) {
      errorHandler.handleFieldIgnored(field);
    }
  }

  private String error(String message, List<Action> buffer) {
    StringBuilder sb = new StringBuilder(message).append(": ");
    for (Action action : buffer) {
      sb.append(action.toDebugString());
    }
    return sb.toString();
  }

  /**
   * @return true when all value is consumed, false when some values is ignored due to the field is not defined in expectedType
   * @throws TException
   */
  private boolean readOneValue(TProtocol in, byte type, List<Action> buffer, ThriftType expectedType)
          throws TException {
    if (expectedType != null && expectedType.getType().getSerializedThriftType() != type) {
      throw new TException("the data type does not match the expected thrift structure: expected " + expectedType + " got " + typeName(type));
    }
    boolean hasFieldsIgnored = false;
    switch (type) {
    case TType.LIST:
      hasFieldsIgnored = readOneList(in, buffer, (ListType)expectedType);
      break;
    case TType.MAP:
      hasFieldsIgnored = readOneMap(in, buffer, (MapType)expectedType);
      break;
    case TType.SET:
      hasFieldsIgnored = readOneSet(in, buffer, (SetType)expectedType);
      break;
    case TType.STRUCT:
      hasFieldsIgnored = readOneStruct(in, buffer, (StructType)expectedType);
      break;
    case TType.STOP:
      break;
    case TType.BOOL:
      final boolean bool = in.readBool();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeBool(bool);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(bool);
        }
      });
      break;
    case TType.BYTE:
      final byte b = in.readByte();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeByte(b);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(b);
        }
      });
      break;
    case TType.DOUBLE:
      final double d = in.readDouble();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeDouble(d);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(d);
        }
      });
      break;
    case TType.I16:
      final short s = in.readI16();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI16(s);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(s);
        }
      });
      break;
    case TType.ENUM: // same as i32 => actually never seen in the protocol layer as enums are written as a i32 field
    case TType.I32:
      final int i = in.readI32();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI32(i);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(i);
        }
      });
      break;
    case TType.I64:
      final long l = in.readI64();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI64(l);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(l);
        }
      });
      break;
    case TType.STRING:
      final ByteBuffer bin = in.readBinary();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeBinary(bin);
        }

        @Override
        public String toDebugString() {
          return String.valueOf(bin);
        }
      });
      break;
    case TType.VOID:
      break;
    default:
      throw new TException("Unknown type: " + type);
    }
    return hasFieldsIgnored;
  }

  private String typeName(byte type) {
    try {
      return ThriftTypeID.fromByte(type).name();
    } catch (RuntimeException e) {
      return String.valueOf(type);
    }
  }

  private boolean readOneStruct(TProtocol in, List<Action> buffer, StructType type) throws TException {
    final TStruct struct = in.readStructBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeStructBegin(struct);
      }

      @Override
      public String toDebugString() {
        return "(";
      }
    });
    TField field;
    boolean hasFieldsIgnored = false;
    while ((field = in.readFieldBegin()).type != TType.STOP) {
      final TField currentField = field;
      if (field.id > type.getChildren().size()) {
        notifyIgnoredFieldsOfRecord(field);
        hasFieldsIgnored |= true;
        //read the value and ignore it, NullProtocol will do nothing
        new ProtocolReadToWrite().readOneValue(in, new NullProtocol(), field.type);
        continue;
      }
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeFieldBegin(currentField);
        }

        @Override
        public String toDebugString() {
          return "f=" + currentField.id + "<t=" + typeName(currentField.type) + ">: ";
        }
      });
      ThriftField expectedField = type.getChildById(field.id);
      try {
        hasFieldsIgnored |= readOneValue(in, field.type, buffer, expectedField.getType());
      } catch (Exception e) {
        throw new TException("Error while reading field " + field + " expected " + expectedField, e);
      }
      in.readFieldEnd();
      buffer.add(FIELD_END);
    }
    in.readStructEnd();
    buffer.add(STRUCT_END);
    return hasFieldsIgnored;
  }

  private boolean readOneMap(TProtocol in, List<Action> buffer, MapType mapType) throws TException {
    final TMap map = in.readMapBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeMapBegin(map);
      }

      @Override
      public String toDebugString() {
        return "<k=" + map.keyType + ", v=" + map.valueType + ", s=" + map.size + ">[";
      }
    });
    boolean hasFieldIgnored = false;
    for (int i = 0; i < map.size; i++) {
      hasFieldIgnored |= readOneValue(in, map.keyType, buffer, mapType.getKey().getType());
      hasFieldIgnored |= readOneValue(in, map.valueType, buffer, mapType.getValue().getType());
    }
    in.readMapEnd();
    buffer.add(MAP_END);
    return hasFieldIgnored;
  }

  private boolean readOneSet(TProtocol in, List<Action> buffer, SetType expectedType) throws TException {
    final TSet set = in.readSetBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeSetBegin(set);
      }

      @Override
      public String toDebugString() {
        return "<e=" + set.elemType + ", s=" + set.size + ">{*";
      }
    });

    boolean hasFieldsIgnored = readCollectionElements(in, set.size, set.elemType, buffer, expectedType.getValues().getType());
    in.readSetEnd();
    buffer.add(SET_END);
    return hasFieldsIgnored;
  }

  private boolean readOneList(TProtocol in, List<Action> buffer, ListType expectedType) throws TException {
    final TList list = in.readListBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeListBegin(list);
      }

      @Override
      public String toDebugString() {
        return "<e=" + list.elemType + ", s=" + list.size + ">{";
      }
    });
    boolean hasFieldsIgnored = readCollectionElements(in, list.size, list.elemType, buffer, expectedType.getValues().getType());
    in.readListEnd();
    buffer.add(LIST_END);
    return hasFieldsIgnored;
  }

  private boolean readCollectionElements(TProtocol in,
                                         final int size, final byte elemType, List<Action> buffer, ThriftType expectedType) throws TException {
    boolean hasFieldIgnored = false;
    for (int i = 0; i < size; i++) {
      hasFieldIgnored |= readOneValue(in, elemType, buffer, expectedType);
    }
    return hasFieldIgnored;
  }

  private interface Action {
    void write(TProtocol out) throws TException;

    String toDebugString();
  }

  /**
   * implements this class to handle errors encountered during reading and writing
   * use {@link #registerErrorHandler(parquet.thrift.BufferedProtocolReadToWrite.ReadWriteErrorHandler)} to register a handler
   */
  public static interface ReadWriteErrorHandler {
    /**
     * handle when a record can not be read due to an exception,
     * in this case the record will be skipped and this method will be called with the exception that caused reading failure
     *
     * @param e
     */
    void handleSkippedCorruptedRecord(SkippableException e);

    /**
     * handle when a record that contains fields that are ignored, meaning that the schema provided does not cover all the columns in data,
     * the record will still be written but with fields that are not defined in the schema ignored.
     * For each record, this method will be called at most once.
     */
    void handleRecordHasFieldIgnored();

    /**
     * handle when a field gets ignored,
     * notice the difference between this method and {@link #handleRecordHasFieldIgnored()} is that:
     * for one record, this method maybe called many times when there are multiple fields not defined in the schema.
     *
     * @param field
     */
    void handleFieldIgnored(TField field);
  }

  /**
   * NullProtocol does nothing when writing to it, used for ignoring unrecognized fields.
   */
  class NullProtocol extends TProtocol {

    public NullProtocol() {
      super(null);
    }

    @Override
    public void writeMessageBegin(TMessage tMessage) throws TException {
    }

    @Override
    public void writeMessageEnd() throws TException {
    }

    @Override
    public void writeStructBegin(TStruct tStruct) throws TException {
    }

    @Override
    public void writeStructEnd() throws TException {
    }

    @Override
    public void writeFieldBegin(TField tField) throws TException {
    }

    @Override
    public void writeFieldEnd() throws TException {
    }

    @Override
    public void writeFieldStop() throws TException {
    }

    @Override
    public void writeMapBegin(TMap tMap) throws TException {
    }

    @Override
    public void writeMapEnd() throws TException {
    }

    @Override
    public void writeListBegin(TList tList) throws TException {
    }

    @Override
    public void writeListEnd() throws TException {
    }

    @Override
    public void writeSetBegin(TSet tSet) throws TException {
    }

    @Override
    public void writeSetEnd() throws TException {
    }

    @Override
    public void writeBool(boolean b) throws TException {
    }

    @Override
    public void writeByte(byte b) throws TException {
    }

    @Override
    public void writeI16(short i) throws TException {
    }

    @Override
    public void writeI32(int i) throws TException {
    }

    @Override
    public void writeI64(long l) throws TException {
    }

    @Override
    public void writeDouble(double v) throws TException {
    }

    @Override
    public void writeString(String s) throws TException {
    }

    @Override
    public void writeBinary(ByteBuffer byteBuffer) throws TException {
    }

    @Override
    public TMessage readMessageBegin() throws TException {
      return null;
    }

    @Override
    public void readMessageEnd() throws TException {
    }

    @Override
    public TStruct readStructBegin() throws TException {
      return null;
    }

    @Override
    public void readStructEnd() throws TException {
    }

    @Override
    public TField readFieldBegin() throws TException {
      return null;
    }

    @Override
    public void readFieldEnd() throws TException {
    }

    @Override
    public TMap readMapBegin() throws TException {
      return null;
    }

    @Override
    public void readMapEnd() throws TException {
    }

    @Override
    public TList readListBegin() throws TException {
      return null;
    }

    @Override
    public void readListEnd() throws TException {
    }

    @Override
    public TSet readSetBegin() throws TException {
      return null;
    }

    @Override
    public void readSetEnd() throws TException {
    }

    @Override
    public boolean readBool() throws TException {
      return false;
    }

    @Override
    public byte readByte() throws TException {
      return 0;
    }

    @Override
    public short readI16() throws TException {
      return 0;
    }

    @Override
    public int readI32() throws TException {
      return 0;
    }

    @Override
    public long readI64() throws TException {
      return 0;
    }

    @Override
    public double readDouble() throws TException {
      return 0;
    }

    @Override
    public String readString() throws TException {
      return null;
    }

    @Override
    public ByteBuffer readBinary() throws TException {
      return null;
    }
  }
}
