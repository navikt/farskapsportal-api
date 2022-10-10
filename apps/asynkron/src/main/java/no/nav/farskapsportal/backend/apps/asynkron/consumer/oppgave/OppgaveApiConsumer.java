package no.nav.farskapsportal.backend.apps.asynkron.consumer.oppgave;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.SIKKER_LOGG;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.libs.dto.oppgave.Oppgaveforespoersel;
import no.nav.farskapsportal.backend.libs.dto.oppgave.OppretteOppgaveRespons;
import no.nav.farskapsportal.backend.libs.felles.consumer.ConsumerEndpoint;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@AllArgsConstructor
public class OppgaveApiConsumer {

  private final RestTemplate restTemplate;
  private final ConsumerEndpoint consumerEndpoint;

  @Retryable(value = RestClientException.class, maxAttempts = 5, backoff = @Backoff(delay = 30000))
  public long oppretteOppgave(Oppgaveforespoersel opprettOppgaveforespoersel) {

    SIKKER_LOGG.debug("oppretter oppgave: " + opprettOppgaveforespoersel);
    log.info("oppretter oppgave med type {}", opprettOppgaveforespoersel.getOppgavetype());

    var oppgaveResponse = restTemplate.postForEntity(
        consumerEndpoint.retrieveEndpoint(OppgaveApiConsumerEndpoint.OPPRETTE_OPPGAVE_ENDPOINT_NAME), new HttpEntity<>(opprettOppgaveforespoersel),
        OppretteOppgaveRespons.class);

    SIKKER_LOGG.debug("oppgaveResponse: " + oppgaveResponse);

    return Optional.of(oppgaveResponse)
        .map(ResponseEntity::getBody)
        .map(OppretteOppgaveRespons::getId)
        .orElse(-1L);
  }
}
