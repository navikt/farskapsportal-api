package no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import no.nav.brukernotifikasjon.schemas.Done;
import no.nav.brukernotifikasjon.schemas.builders.DoneBuilder;
<<<<<<< HEAD:libs/felles/src/main/java/no/nav/farskapsportal/backend/libs/felles/consumer/brukernotifikasjon/Ferdigprodusent.java
import no.nav.brukernotifikasjon.schemas.builders.NokkelBuilder;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
=======
import no.nav.farskapsportal.api.Feilkode;
import no.nav.farskapsportal.config.egenskaper.FarskapsportalEgenskaper;
import no.nav.farskapsportal.exception.InternFeilException;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.OppgavebestillingDao;
import no.nav.farskapsportal.persistence.entity.Forelder;
import no.nav.farskapsportal.service.PersistenceService;
>>>>>>> main:src/main/java/no/nav/farskapsportal/consumer/brukernotifikasjon/Ferdigprodusent.java
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
@Value
public class Ferdigprodusent {

  FarskapsportalFellesEgenskaper farskapsportalFellesEgenskaper;
  KafkaTemplate kafkaTemplate;
<<<<<<< HEAD:libs/felles/src/main/java/no/nav/farskapsportal/backend/libs/felles/consumer/brukernotifikasjon/Ferdigprodusent.java

  public void ferdigstilleFarsSigneringsoppgave(String idFarskapserklaering, String foedselsnummerFar) {

    var nokkel = new NokkelBuilder().withEventId(idFarskapserklaering).withSystembruker(farskapsportalFellesEgenskaper.getSystembrukerBrukernavn())
        .build();
    var melding = oppretteDone(foedselsnummerFar);

    kafkaTemplate.send(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicFerdig(), nokkel, melding);
=======
  PersistenceService persistenceService;
  OppgavebestillingDao oppgavebestillingDao;

  public void ferdigstilleFarsSigneringsoppgave(Forelder far, Nokkel nokkel) {

    var oppgaveSomSkalFerdigstilles = oppgavebestillingDao.henteOppgavebestilling(nokkel.getEventId());

    if (oppgaveSomSkalFerdigstilles.isPresent() && oppgaveSomSkalFerdigstilles.get().getFerdigstilt() == null) {
      var melding = oppretteDone(far.getFoedselsnummer());
      try {
        kafkaTemplate.send(farskapsportalEgenskaper.getBrukernotifikasjon().getTopicFerdig(), nokkel, melding);
      } catch (Exception e) {
        throw new InternFeilException(Feilkode.BRUKERNOTIFIKASJON_OPPRETTE_OPPGAVE, e);
      }

      log.info("Ferdigmelding ble sendt for oppgave med eventId {}.");
      persistenceService.setteOppgaveTilFerdigstilt(nokkel.getEventId());
    } else {
      log.warn("Fant ingen aktiv oppgavebestilling for eventId {} (gjelder far med id: {}). Bestiller derfor ikke ferdigstilling.",
          nokkel.getEventId(), far.getId());
    }
>>>>>>> main:src/main/java/no/nav/farskapsportal/consumer/brukernotifikasjon/Ferdigprodusent.java
  }

  private Done oppretteDone(String foedselsnummerFar) {
    return new DoneBuilder()
        .withTidspunkt(ZonedDateTime.now(ZoneId.of("UTC")).toLocalDateTime())
        .withFodselsnummer(foedselsnummerFar)
        .withGrupperingsId(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getGrupperingsidFarskap())
        .build();
  }
}
