package no.nav.farskapsportal.exception;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.bidrag.commons.ExceptionLogger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;

@RestControllerAdvice
@AllArgsConstructor
@Slf4j
public class RestResponseExceptionResolver {

  private final ExceptionLogger exceptionLogger;

  @ResponseBody
  @ExceptionHandler(RestClientException.class)
  protected ResponseEntity<?> handleRestClientException(RestClientException e) {
    exceptionLogger.logException(e, "RestResponseExceptionResolver");

    var feilmelding = "Restkall feilet!";

    HttpHeaders headers = new HttpHeaders();
    headers.add(HttpHeaders.WARNING, feilmelding);

    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .body(new ResponseEntity<>(e.getMessage(), headers, HttpStatus.SERVICE_UNAVAILABLE));
  }
}
