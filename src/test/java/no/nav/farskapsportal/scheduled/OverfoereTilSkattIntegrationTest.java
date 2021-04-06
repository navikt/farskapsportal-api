package no.nav.farskapsportal.scheduled;

import static no.nav.farskapsportal.FarskapsportalApplication.PROFILE_SCHEDULED_TEST;
import static no.nav.farskapsportal.TestUtils.henteBarnMedFnr;
import static no.nav.farskapsportal.TestUtils.henteFarskapserklaering;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import no.nav.farskapsportal.FarskapsportalApplicationLocal;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.config.ScheduledConfig;
import no.nav.farskapsportal.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.dao.MeldingsloggDao;
import no.nav.farskapsportal.service.PersistenceService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ActiveProfiles(PROFILE_SCHEDULED_TEST)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = FarskapsportalApplicationLocal.class)
@SpringJUnitConfig(ScheduledConfig.class)
public class OverfoereTilSkattIntegrationTest {

  private static final ForelderDto MOR = henteForelder(Forelderrolle.MOR);
  private static final ForelderDto FAR = henteForelder(Forelderrolle.FAR);

  @MockBean
  private SkattConsumer skattConsumer;

  @Autowired
  private PersistenceService persistenceService;

  @Autowired
  private FarskapserklaeringDao farskapserklaeringDao;

  @Autowired
  private MeldingsloggDao meldingsloggDao;

  @Value("${farskapsportal.egenskaper.skatt.intervall.overfoering}")
  private long intervall;

  @Test
  public void skalPlukkeOppOgSendeFerdigstilteFarskapserklaeringerTilSkatt() throws InterruptedException {

    // given
    var alleFarskapserklaeringer = farskapserklaeringDao.findAll();
    assertFalse(alleFarskapserklaeringer.iterator().hasNext());

    var sendingsklarFarskapserklaering1 = persistenceService
        .lagreNyFarskapserklaering(henteFarskapserklaering(MOR, FAR, henteBarnMedFnr(LocalDate.now().minusMonths(1), "11111")));
    // Bare farskapserklæringer som er signert av far, og har satt meldingsIdSkatt er aktuelle for overføring
    sendingsklarFarskapserklaering1.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now().minusMinutes(10));
    sendingsklarFarskapserklaering1.setMeldingsidSkatt(4564l);
    assertNull(sendingsklarFarskapserklaering1.getSendtTilSkatt());
    farskapserklaeringDao.save(sendingsklarFarskapserklaering1);

    var sendingsklarFarskapserklaering2 = persistenceService
        .lagreNyFarskapserklaering(henteFarskapserklaering(MOR, FAR, henteBarnMedFnr(LocalDate.now().minusMonths(1), "22222")));
    // Bare farskapserklæringer som er signert av far, og har satt meldingsIdSkatt er aktuelle for overføring
    sendingsklarFarskapserklaering2.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now().minusMinutes(10));
    sendingsklarFarskapserklaering2.setMeldingsidSkatt(4564l);
    assertNull(sendingsklarFarskapserklaering2.getSendtTilSkatt());
    farskapserklaeringDao.save(sendingsklarFarskapserklaering2);

    var ikkeSendingsklarFarskapserklaering = persistenceService
        .lagreNyFarskapserklaering(henteFarskapserklaering(MOR, FAR, henteBarnMedFnr(LocalDate.now().minusMonths(1), "33333")));
    // Bare farskapserklæringer som er signert av far, og har satt meldingsIdSkatt er aktuelle for overføring
    ikkeSendingsklarFarskapserklaering.getDokument().getSigneringsinformasjonMor().setSigneringstidspunkt(LocalDateTime.now().minusMinutes(15));
    farskapserklaeringDao.save(ikkeSendingsklarFarskapserklaering);
    assertNull(ikkeSendingsklarFarskapserklaering.getSendtTilSkatt());

    // when
    Thread.sleep(intervall * 2);

    var fe1 = farskapserklaeringDao.findById(sendingsklarFarskapserklaering1.getId());
    var fe2 = farskapserklaeringDao.findById(sendingsklarFarskapserklaering2.getId());
    var fe3 = farskapserklaeringDao.findById(ikkeSendingsklarFarskapserklaering.getId());

    assertAll(
        () -> assertThat(fe1).isPresent(),
        () -> assertNotNull(fe1.get().getMeldingsidSkatt()),
        () -> assertNotNull(fe1.get().getSendtTilSkatt(), "Tidspunkt for oversendelse skal være satt for erklæringer som er sendt til Skatt"),
        () -> assertTrue(meldingsIdFinnesIMeldingslogg(fe1.get().getMeldingsidSkatt())),
        () -> assertThat(fe2).isPresent(),
        () -> assertNotNull(fe2.get().getSendtTilSkatt(), "Tidspunkt for oversendelse skal være satt for erklæringer som er sendt til Skatt"),
        () -> assertNotNull(fe2.get().getMeldingsidSkatt()),
        () -> assertTrue(meldingsIdFinnesIMeldingslogg(fe2.get().getMeldingsidSkatt())),
        () -> assertThat(fe3).isPresent(),
        () -> assertNull(fe3.get().getSendtTilSkatt(), "Tidspunkt for oversendelse skal IKKE være satt for erklæringer som IKKE er sendt til Skatt"),
        () -> assertNotNull(fe3.get().getMeldingsidSkatt()),
        () -> assertFalse(meldingsIdFinnesIMeldingslogg(fe3.get().getMeldingsidSkatt()),
            "Ingen innslag i oversendelseslogg for farskapserklæring som ikke er sendt til Skatt")
    );
  }

  private boolean meldingsIdFinnesIMeldingslogg(long meldingsidSkatt) {
    var meldingsloggAlleInnslag = meldingsloggDao.findAll().iterator();
    while (meldingsloggAlleInnslag.hasNext()) {
      var innslag = meldingsloggAlleInnslag.next();
      if (innslag.getMeldingsidSkatt() == meldingsidSkatt) {
        return true;
      }
    }
    return false;
  }
}
