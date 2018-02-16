package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface CheckedRunnable<X extends Exception> {
  void run() throws X;
  
  /**
   *  A no-op.
   */
  static void nop() {}
  
  static CheckedRunnable<RuntimeException> wrap(Runnable runnable) {
    return runnable::run;
  }
}