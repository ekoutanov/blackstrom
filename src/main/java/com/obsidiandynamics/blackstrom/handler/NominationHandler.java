package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface NominationHandler {
  void onNomination(MessageContext context, Nomination nomination);
}
