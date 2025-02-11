package no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon;

import static no.nav.farskapsportal.backend.libs.felles.config.BrukernotifikasjonConfig.NAMESPACE_FARSKAPSPORTAL;
import static no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon.BrukernotifikasjonConsumer.MELDING_OM_VENTENDE_FARSKAPSERKLAERING;

import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.libs.entity.Forelder;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
import no.nav.farskapsportal.backend.libs.felles.exception.Feilkode;
import no.nav.farskapsportal.backend.libs.felles.exception.InternFeilException;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import no.nav.tms.varsel.action.Sensitivitet;
import no.nav.tms.varsel.action.Varseltype;
import no.nav.tms.varsel.builder.OpprettVarselBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
@AllArgsConstructor
public class Varselprodusent {
  private KafkaTemplate<String, String> kafkaTemplate;
  private PersistenceService persistenceService;
  private URL farskapsportalUrl;
  private FarskapsportalFellesEgenskaper farskapsportalFellesEgenskaper;

  public void oppretteOppgaveForSigneringAvFarskapserklaering(
      int idFarskapserklaering, Forelder far) {

    var varselid = UUID.randomUUID().toString();

    var farsAktiveSigneringsoppgaver =
        persistenceService.henteAktiveOppgaverTilForelderIFarskapserklaering(
            idFarskapserklaering, far);

    if (farsAktiveSigneringsoppgaver.isEmpty()) {
      log.info(
          "Oppretter oppgave om signering til far i farskapserklæring med id {}",
          idFarskapserklaering);
      oppretteOppgave(varselid, oppretteVarselForOppgave(varselid, far.getFoedselsnummer()));
      log.info("Signeringsppgave opprettet for far med id {}.", far.getId());
      persistenceService.lagreNyOppgavebestilling(idFarskapserklaering, varselid);
    }
  }

  private void oppretteOppgave(String varselid, String melding) {
    try {
      kafkaTemplate.send(
          farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicOppgave(),
          varselid,
          melding);
    } catch (Exception e) {
      throw new InternFeilException(Feilkode.BRUKERNOTIFIKASJON_OPPRETTE_OPPGAVE, e);
    }
  }

  private String oppretteVarselForOppgave(String varselid, String personident) {
    return OpprettVarselBuilder.newInstance()
        .withType(Varseltype.Oppgave)
        .withVarselId(varselid)
        .withIdent(personident)
        .withSensitivitet(Sensitivitet.High)
        .withAktivFremTil(
            ZonedDateTime.now(ZoneId.of("UTC"))
                .plusDays(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getLevetidOppgaveAntallDager()))
        // Vurdere å angi prioritering av kanal og spesifisering av tekst for SMS og epost
        .withEksternVarsling()
        .withLink(farskapsportalUrl.toString())
        .withTekst("nb", MELDING_OM_VENTENDE_FARSKAPSERKLAERING)
        .withProdusent(
            farskapsportalFellesEgenskaper.getNaisClusternavn(),
            NAMESPACE_FARSKAPSPORTAL,
            farskapsportalFellesEgenskaper.getAppnavn())
        .build();
  }
}
