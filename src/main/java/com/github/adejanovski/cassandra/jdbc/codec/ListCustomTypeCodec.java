package com.github.adejanovski.cassandra.jdbc.codec;

import com.datastax.driver.core.CodecUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeTokens;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListCustomTypeCodec extends TypeCodec<List<String>> {

  private static final Logger logger = LoggerFactory.getLogger(ListCustomTypeCodec.class);

  public ListCustomTypeCodec() {
    super(DataType.list(DataType.custom("udt")), TypeTokens.listOf(String.class));
  }

  @Override
  public ByteBuffer serialize(List<String> paramT, ProtocolVersion paramProtocolVersion)
      throws InvalidTypeException {
    logger.debug("SERIALIZE: " + paramT);
    if (paramT == null) {
      return null;
    }
    return ByteBufferUtil.bytes(paramT.toString());
  }

  @Override
  public List<String> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
      throws InvalidTypeException {
    if (bytes == null) {
      return null;

    }
    try {
      ByteBuffer input = bytes.duplicate();
      int n = CodecUtils.readSize(input, protocolVersion);
      List<String> values = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {

        ByteBuffer vbb = CodecUtils.readValue(input, protocolVersion);

//        final List<Byte> byteStr = new ArrayList<>();
//        for (byte b : vbb.array()) {
//          if (b <= ' ') {
//            byteStr.add(b);
//          }
//        }
//        byte[] b = new byte[byteStr.size()];
//        for (int j = 0; j < byteStr.size(); j++) {
//          b[j] = byteStr.get(j);
//        }

        try {
          String deserialized;
          try {
//            deserialized = ByteBufferUtil.string(bytes, StandardCharsets.UTF_8)
            deserialized = UTF8Serializer.instance
                .deserialize(vbb)
                .trim()
                .replaceAll("[^\\x20-\\x7e]", " ");
          } catch (Exception e) {
            deserialized = "Decoding error";
          }

          // replace non ascii
//              .replaceAll("[^\\u0000-\\uFFFF]", " "); // non-four-byte-UTF-8 characters
          final String value = Arrays.stream(deserialized.split(" "))
              .filter(str -> str != null && !str.isEmpty())
              .map(String::trim)
              .filter(str -> !str.isEmpty())
              .collect(Collectors.joining(" "));
          logger.debug("VALUE: " + value);
          values.add(value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return values;
    } catch (BufferUnderflowException e) {
      throw new InvalidTypeException("Not enough bytes to deserialize a map", e);
    }

//    // always duplicate the ByteBuffer instance before consuming it!
//    try {
//      final String string = ByteBufferUtil.string(bytes.duplicate());
//      logger.debug("STRING RECEIVED: " + string);
//      return Collections.singletonList(string);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
  }

  @Override
  public List<String> parse(String paramString) throws InvalidTypeException {
    logger.debug("PARSE: " + paramString);
    return Collections.singletonList(paramString);
  }

  @Override
  public String format(List<String> paramT) throws InvalidTypeException {
    logger.debug("FORMAT: " + paramT);
    return paramT.toString();
  }

  @Override
  public boolean accepts(DataType cqlType) {
    logger.debug("cqlType: " + cqlType);
    logger.debug("cqlType name: " + cqlType.getName().name());
    logger.debug("true?" + cqlType.getName().name().equals("UDT"));
    logger.debug("cqlType type arguments: " + cqlType.getTypeArguments());
    final String name = cqlType.getName().name();
    logger.debug(
        "list: " + (name.equals("LIST") && cqlType.getTypeArguments().get(0).getName().name()
            .equals("UDT")));
    return (name.equals("LIST") && cqlType.getTypeArguments().get(0).getName().name()
        .equals("UDT"));
  }

  @Override
  public boolean accepts(TypeToken<?> javaType) {
    logger.debug("type token: " + javaType);
    return true;
  }

  @Override
  public boolean accepts(Class<?> javaType) {
    logger.debug("java type: " + javaType);
    return true;
  }
}
