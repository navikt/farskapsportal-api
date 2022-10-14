package no.nav.farskapsportal.backend.apps.asynkron.config;

import static no.nav.farskapsportal.backend.apps.asynkron.config.FarskapsportalAsynkronConfig.PROFILE_SCHEDULED_TEST;
import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_LIVE;

import no.nav.farskapsportal.backend.apps.asynkron.config.egenskaper.FarskapsportalAsynkronEgenskaper;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.api.FarskapsportalApiConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.oppgave.OppgaveApiConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.arkiv.ArkivereFarskapserklaeringer;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.brukernotifikasjon.Brukernotifikasjonstyring;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.arkiv.DeaktivereFarskapserklaeringer;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.esignering.OppdatereSigneringsstatus;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.oppgave.Oppgavestyring;
import no.nav.farskapsportal.backend.apps.asynkron.scheduled.brukernotifikasjon.Varsel;
import no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon.BrukernotifikasjonConsumer;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Profile({PROFILE_LIVE, PROFILE_SCHEDULED_TEST})
@Configuration
@EnableScheduling
@ComponentScan
public class ScheduledConfig {

  private FarskapsportalAsynkronEgenskaper farskapsportalAsynkronEgenskaper;

  public ScheduledConfig(@Autowired FarskapsportalAsynkronEgenskaper farskapsportalAsynkronEgenskaper) {
    this.farskapsportalAsynkronEgenskaper = farskapsportalAsynkronEgenskaper;
  }

  @Bean
  public ArkivereFarskapserklaeringer arkivereFarskapserklaeringer(
      PersistenceService persistenceService,
      SkattConsumer skattConsumer) {

    return ArkivereFarskapserklaeringer.builder()
        .intervallMellomForsoek(farskapsportalAsynkronEgenskaper.getArkiv().getArkiveringsintervall())
        .persistenceService(persistenceService)
        .skattConsumer(skattConsumer)
        .build();
  }

  @Bean
  public DeaktivereFarskapserklaeringer deaktivereFarskapserklaeringer(
      BrukernotifikasjonConsumer brukernotifikasjonConsumer,
      PersistenceService persistenceService) {
    return DeaktivereFarskapserklaeringer.builder()
        .brukernotifikasjonConsumer(brukernotifikasjonConsumer)
        .egenskaperArkiv(farskapsportalAsynkronEgenskaper.getArkiv())
        .persistenceService(persistenceService).build();
  }

  @Bean
  public OppdatereSigneringsstatus oppdatereSigneringsstatus(PersistenceService persistenceService,
      FarskapsportalApiConsumer farskapsportalApiConsumer) {

    return OppdatereSigneringsstatus.builder()
        .farskapsportalApiConsumer(farskapsportalApiConsumer)
        .farskapsportalAsynkronEgenskaper(farskapsportalAsynkronEgenskaper)
        .persistenceService(persistenceService).build();
  }

  @Bean
  public Brukernotifikasjonstyring brukernotifikasjonstyring(
      BrukernotifikasjonConsumer brukernotifikasjonConsumer,
      FarskapserklaeringDao farskapserklaeringDao,
      PersistenceService persistenceService) {

    return Brukernotifikasjonstyring.builder()
        .brukernotifikasjonConsumer(brukernotifikasjonConsumer)
        .farskapserklaeringDao(farskapserklaeringDao)
        .persistenceService(persistenceService)
        .build();
  }

  @Bean
  public Varsel varsel(
      BrukernotifikasjonConsumer brukernotifikasjonConsumer,
      PersistenceService persistenceService
  ) {

    return Varsel.builder()
        .brukernotifikasjonConsumer(brukernotifikasjonConsumer)
        .egenskaperBrukernotifikasjon(farskapsportalAsynkronEgenskaper.getBrukernotifikasjon())
        .persistenceService(persistenceService)
        .build();
  }

  @Bean
  public Oppgavestyring oppgavestyring(
      FarskapsportalApiConsumer farskapsportalApiConsumer,
      FarskapserklaeringDao farskapserklaeringDao,
      OppgaveApiConsumer oppgaveApiConsumer
  ) {
    return Oppgavestyring.builder()
        .farskapsportalApiConsumer(farskapsportalApiConsumer)
        .egenskaperOppgavestyring(farskapsportalAsynkronEgenskaper.getOppgave())
        .farskapserklaeringDao(farskapserklaeringDao)
        .oppgaveApiConsumer(oppgaveApiConsumer)
        .build();
  }
}
