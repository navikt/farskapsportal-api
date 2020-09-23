package no.nav.farskapsportal.exception;

public class UnrecoverableException extends RuntimeException {
  public UnrecoverableException(String message) {
    super(message);
  }

  public UnrecoverableException(String message, Exception e) {
    super(message, e);
  }
}
