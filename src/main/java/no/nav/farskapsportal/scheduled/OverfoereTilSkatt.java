package no.nav.farskapsportal.scheduled;

import java.time.LocalDateTime;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.exception.SkattConsumerException;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
import no.nav.farskapsportal.service.PersistenceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.validation.annotation.Validated;

@Builder
@Validated
@Slf4j
public class OverfoereTilSkatt {

  private PersistenceService persistenceService;
  private SkattConsumer skattConsumer;
  private int intervallMellomForsoek;

  @Scheduled(initialDelay = 60000, fixedDelayString = "${farskapsportal.egenskaper.skatt.intervall-overfoering}")
  void vurdereOverfoeringTilSkatt() {
    log.info("Ser etter ferdigstilte farskapserklæringer som skal overføres til skatt");
    var farskapserklaeringer = persistenceService.henteFarskapserklaeringerSomErKlareForOverfoeringTilSkatt();
    log.info("Fant {} farskapserklæringer som er klar til overføring.", farskapserklaeringer.size());
    for (Farskapserklaering fe : farskapserklaeringer) {
      log.debug("Oppdaterer tidspunkt for oversendelse til skatt for farskapserklæring med id {}", fe.getId());
      fe.setSendtTilSkatt(LocalDateTime.now());
      try {
        skattConsumer.registrereFarskap(fe);
      } catch (SkattConsumerException sce) {

        var tidspunktNesteForsoek = LocalDateTime.now().plusSeconds(intervallMellomForsoek / 1000);

        log.error(
            "En feil oppstod i kommunikasjon med Skatt. Farskapserklæring med meldingsidSkatt {} ble ikke overført. Nytt forsøk vil bli igangsatt kl {}",
            fe.getMeldingsidSkatt(), tidspunktNesteForsoek, sce);
        throw sce;
      }
      fe.setSendtTilSkatt(LocalDateTime.now());
      persistenceService.oppdatereFarskapserklaering(fe);
      persistenceService.oppdatereMeldingslogg(fe.getSendtTilSkatt(), fe.getMeldingsidSkatt());
      log.debug("Meldingslogg oppdatert");
    }
    log.info("Farskapserklæringene ble overført til Skatt uten problemer");
  }
}
