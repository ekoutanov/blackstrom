package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface VoteHandler {
  void onVote(MessageContext context, Vote vote);
}
