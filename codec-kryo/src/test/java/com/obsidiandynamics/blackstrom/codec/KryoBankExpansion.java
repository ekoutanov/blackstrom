package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.codec.KryoMessageCodec.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class KryoBankExpansion implements KryoExpansion {
  @Override
  public void accept(Kryo kryo) {
    kryo.addDefaultSerializer(BankSettlement.class, KryoBankSettlementSerializer.class);
  }
}
