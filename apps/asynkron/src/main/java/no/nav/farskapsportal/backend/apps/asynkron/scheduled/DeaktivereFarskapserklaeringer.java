package no.nav.farskapsportal.backend.apps.asynkron.scheduled;

import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.apps.asynkron.config.egenskaper.FarskapsportalAsynkronEgenskaper;
import no.nav.farskapsportal.backend.libs.entity.Oppgavebestilling;
import no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon.BrukernotifikasjonConsumer;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@Builder
public class DeaktivereFarskapserklaeringer {

  private BrukernotifikasjonConsumer brukernotifikasjonConsumer;
  private FarskapsportalAsynkronEgenskaper farskapsportalAsynkronEgenskaper;
  private PersistenceService persistenceService;

  @Scheduled(cron = "${farskapsportal.asynkron.egenskaper.deaktiveringsrate}", zone = "Europe/Oslo")
  public void deaktivereFarskapserklaeringer() {
    deaktivereFarskapserklaeringerMedUtgaatteSigneringsoppdrag();
    deaktivereFarskapserklaeringerSomErSendtTilSkatt();
    deaktivereFarskapserklaeringerSomManglerMorsSignatur();
  }

  private void deaktivereFarskapserklaeringerMedUtgaatteSigneringsoppdrag() {
    var eldsteGyldigeDatoForSigneringsoppdrag = LocalDate.now()
        .minusDays(farskapsportalAsynkronEgenskaper.getLevetidIkkeFerdigstilteSigneringsoppdragIDager() < 40 ? 40
            : farskapsportalAsynkronEgenskaper.getLevetidIkkeFerdigstilteSigneringsoppdragIDager());
    var utloepstidspunkt = eldsteGyldigeDatoForSigneringsoppdrag.atStartOfDay();

    var idTilFarskapserklaeringerMedUtgaatteSigneringsoppdrag = persistenceService.henteIdTilAktiveFarskapserklaeringerMedUtgaatteSigneringsoppdrag(
        utloepstidspunkt);

    log.info("Fant {} ikke-ferdigstilte farskapserklæringer med signeringsoppdrag eldre enn 40 dager. Deaktiverer disse.",
        idTilFarskapserklaeringerMedUtgaatteSigneringsoppdrag.size());

    for (int farskapserklaeringsid : idTilFarskapserklaeringerMedUtgaatteSigneringsoppdrag) {
      var farskapserklaering = persistenceService.henteFarskapserklaeringForId(farskapserklaeringsid);
      var aktiveOppgaver = persistenceService.henteAktiveOppgaverTilForelderIFarskapserklaering(farskapserklaeringsid,
          farskapserklaering.getFar());
      log.info("Fant {} utløpte signeringsoppgaver knyttet til farskapserklæring med id {}.", aktiveOppgaver.size(), farskapserklaeringsid);
      for (Oppgavebestilling oppgave : aktiveOppgaver) {
        log.info("Sletter utgått signeringsoppgave for far (id {}) i farskapserklæring (id {})", farskapserklaering.getFar().getId(),
            farskapserklaeringsid);
        brukernotifikasjonConsumer.sletteFarsSigneringsoppgave(oppgave.getEventId(), farskapserklaering.getFar());
      }

      persistenceService.deaktivereFarskapserklaering(farskapserklaeringsid);
      log.info("Sletter dokumentinnhold til farskapserklæring med id {}", farskapserklaeringsid);
      persistenceService.sletteDokumentinnhold(farskapserklaeringsid);

      if (aktiveOppgaver.size() > 0) {
        log.info("Varsler mor om utgått signeringsoppave");
        brukernotifikasjonConsumer.varsleMorOmUtgaattOppgaveForSignering(farskapserklaering.getMor());
      }
    }

    log.info(
        "Farskapserklæringer med utgåtte signeringsoppdrag deaktivert, relaterte dokumenter er slettet, mor er varslet om utgått signeringsoppgave");
  }

  private void deaktivereFarskapserklaeringerSomErSendtTilSkatt() {
    var antallErklaeringerSomBleDeaktivert = 0;

    var oversendtTilSkattFoer = LocalDate.now()
        .minusDays(farskapsportalAsynkronEgenskaper.getLevetidOversendteFarskapserklaeringerIDager() < 30 ? 30
            : farskapsportalAsynkronEgenskaper.getLevetidOversendteFarskapserklaeringerIDager());
    var tidspunktOversendtFoer = oversendtTilSkattFoer.atStartOfDay();

    var idTilFarskapserklaeringerSomSkalDeaktiveres = persistenceService.henteIdTilOversendteFarskapserklaeringerSomErKlarForDeaktivering(
        tidspunktOversendtFoer);
    log.info("Fant {} ferdigstilte farskapserklæringer som har blitt overført til skatt og er klare for deaktivering.",
        idTilFarskapserklaeringerSomSkalDeaktiveres.size());
    for (int farskapserklaeringsid : idTilFarskapserklaeringerSomSkalDeaktiveres) {
      antallErklaeringerSomBleDeaktivert =
          persistenceService.deaktivereFarskapserklaering(farskapserklaeringsid) ? ++antallErklaeringerSomBleDeaktivert
              : antallErklaeringerSomBleDeaktivert;
    }

    log.info("{} ferdigstilte farskapserklæringer ble i denne omgang deaktivert", antallErklaeringerSomBleDeaktivert);
  }

  private void deaktivereFarskapserklaeringerSomManglerMorsSignatur() {
    var antallErklaeringerSomBleDeaktivert = 0;

    var morSendtTilSigneringFoer = LocalDateTime.now()
        .minusDays(farskapsportalAsynkronEgenskaper.getLevetidErklaeringerIkkeSignertAvMorIDager());

    var idTilFarskapserklaeringerSomSkalDeaktiveres = persistenceService.henteIdTilFarskapserklaeringerSomManglerMorsSignatur(
        morSendtTilSigneringFoer);
    log.info("Fant {} farskapserklæringer som ikke er signert av mor - deaktiverer disse.", idTilFarskapserklaeringerSomSkalDeaktiveres.size());
    for (int farskapserklaeringsid : idTilFarskapserklaeringerSomSkalDeaktiveres) {
      antallErklaeringerSomBleDeaktivert =
          persistenceService.deaktivereFarskapserklaering(farskapserklaeringsid) ? ++antallErklaeringerSomBleDeaktivert
              : antallErklaeringerSomBleDeaktivert;
    }

    log.info("{} usignerte farskapserklæringer ble i denne omgang deaktivert", antallErklaeringerSomBleDeaktivert);
  }
}
