package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;

public interface MessageContext {
  Ledger getLedger();
  
  Object getHandlerId();
  
  Retention getRetention();
  
  default Confirmation begin(Message message) {
    return getRetention().begin(this, message);
  }
  
  default void beginAndConfirm(Message message) {
    begin(message).confirm();
  }
}
