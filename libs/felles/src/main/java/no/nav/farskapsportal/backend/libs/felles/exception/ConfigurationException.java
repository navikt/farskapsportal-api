package no.nav.farskapsportal.backend.libs.felles.exception;

public class ConfigurationException extends RuntimeException {

  public ConfigurationException(String message) {
    this(message, (Exception) null);
  }

  public ConfigurationException(String message, Exception e) {
    super(message, e);
  }

}
