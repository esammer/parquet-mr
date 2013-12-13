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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.protocol.TField;
import parquet.thrift.test.compat.StructV2;
import parquet.thrift.test.compat.StructV3;
import parquet.thrift.test.compat.StructV4WithExtracStructField;
import thrift.test.OneOfEach;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.thrift.test.TestMapInSet;

public class TestProtocolReadToWrite {

  @Test
  public void testOneOfEach() throws Exception {
    OneOfEach a = new OneOfEach(
        true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
        ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    writeReadCompare(a);
  }

  @Test
  public void testWriteRead() throws Exception {
    ArrayList<Person> persons = new ArrayList<Person>();
    final PhoneNumber phoneNumber = new PhoneNumber("555 999 9998");
    phoneNumber.type = PhoneType.HOME;
    persons.add(
        new Person(
            new Name("Bob", "Roberts"),
            1,
            "bob@roberts.com",
            Arrays.asList(new PhoneNumber("555 999 9999"), phoneNumber)));
    persons.add(
        new Person(
            new Name("Dick", "Richardson"),
            2,
            "dick@richardson.com",
            Arrays.asList(new PhoneNumber("555 999 9997"), new PhoneNumber("555 999 9996"))));
    AddressBook a = new AddressBook(persons);
    writeReadCompare(a);
  }

  @Test
  public void testEmptyStruct() throws Exception {
    AddressBook a = new AddressBook();
    writeReadCompare(a);
  }

  @Test
  public void testMapSet() throws Exception {
    final Set<Map<String, String>> set = new HashSet<Map<String,String>>();
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    set.add(map);
    TestMapInSet a = new TestMapInSet("top", set);
    writeReadCompare(a);
  }

  private void writeReadCompare(TBase<?,?> a)
      throws TException, InstantiationException, IllegalAccessException {
    ProtocolPipe[] pipes = {new ProtocolReadToWrite(), new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType((Class<TBase<?,?>>)a.getClass()))};
    for (ProtocolPipe p : pipes) {
      final ByteArrayOutputStream in = new ByteArrayOutputStream();
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      a.write(protocol(in));
      p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
      TBase<?,?> b = a.getClass().newInstance();
      b.read(protocol(new ByteArrayInputStream(out.toByteArray())));

      assertEquals(p.getClass().getSimpleName(), a, b);
    }
  }



  @Test
  public void testIncompatibleRecord() throws Exception {
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(AddressBook.class));
    p.unregisterAllHandlers();
    //handler will rethrow the exception for verifying purpose
    p.registerErrorHandler(new BufferedProtocolReadToWrite.ReadWriteErrorHandler() {
      @Override
      public void handleSkippedCorruptedRecords(RuntimeException e) {
       throw e;
      }
      @Override
      public void handleRecordHasFieldIgnored() {
      }
      @Override
      public void handleFieldIgnored(TField field) {
      }
    });

    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OneOfEach a = new OneOfEach(
        true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
        ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    a.write(protocol(in));
    try {
      p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
      fail("exception expected");
    } catch (SkippableException e) {
      Assert.assertEquals("Error while reading: (f=1<t=BOOL>: ", e.getMessage());
    }
  }

  class CountingErrorHandler implements BufferedProtocolReadToWrite.ReadWriteErrorHandler{
     int corruptedCount=0;
     int fieldIgnoredCount=0;
     int recordCountOfMissingFields=0;
    @Override
    public void handleSkippedCorruptedRecords(RuntimeException e) {
      corruptedCount++;
    }

    @Override
    public void handleRecordHasFieldIgnored() {
      recordCountOfMissingFields++;
    }

    @Override
    public void handleFieldIgnored(TField field) {
      fieldIgnoredCount++;
    }
  }

  @Test
  public void testMissingFieldHandling() throws Exception {
    //write using StructV3
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(StructV3.class));
    //handler will rethrow the exception for verifying purpose

    CountingErrorHandler countingHandler= new CountingErrorHandler();
    p.unregisterAllHandlers();
    p.registerErrorHandler(countingHandler);

    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    StructV4WithExtracStructField dataWithNewSchema= new StructV4WithExtracStructField("name");
    dataWithNewSchema.setAge("10");
    dataWithNewSchema.setGender("male");

    StructV3 structV3=new StructV3("name");
    structV3.setAge("10");

    dataWithNewSchema.setAddedStruct(structV3);
    dataWithNewSchema.write(protocol(in));
    //read using StructV2
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
    assertEquals(0,countingHandler.corruptedCount);
    assertEquals(1,countingHandler.recordCountOfMissingFields);
    assertEquals(1,countingHandler.fieldIgnoredCount);
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }
}
