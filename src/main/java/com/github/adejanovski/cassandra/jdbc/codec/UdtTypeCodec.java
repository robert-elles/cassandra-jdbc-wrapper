package com.github.adejanovski.cassandra.jdbc.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdtTypeCodec extends TypeCodec<String> {

  private static final Logger logger = LoggerFactory.getLogger(UdtTypeCodec.class);

  public UdtTypeCodec() {
    super(DataType.custom("UDT"), String.class);
  }

  @Override
  public ByteBuffer serialize(String paramT, ProtocolVersion paramProtocolVersion)
      throws InvalidTypeException {
    if (paramT == null) {
      return null;
    }
    return ByteBufferUtil.bytes(paramT);
  }

  @Override
  public String deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion)
      throws InvalidTypeException {
    if (paramByteBuffer == null) {
      return null;

    }
    // always duplicate the ByteBuffer instance before consuming it!
    try {
      return ByteBufferUtil.string(paramByteBuffer.duplicate());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String parse(String paramString) throws InvalidTypeException {
    return paramString;
  }

  @Override
  public String format(String paramT) throws InvalidTypeException {
    return paramT;
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
    return name.equals("UDT");
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
