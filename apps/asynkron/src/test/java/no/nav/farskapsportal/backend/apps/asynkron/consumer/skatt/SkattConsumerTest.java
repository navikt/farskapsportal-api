package no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_TEST;
import static no.nav.farskapsportal.backend.libs.felles.exception.Feilkode.DOKUMENT_MANGLER_INNOHLD;
import static no.nav.farskapsportal.backend.libs.felles.exception.Feilkode.XADES_FAR_UTEN_INNHOLD;
import static no.nav.farskapsportal.backend.libs.felles.exception.Feilkode.XADES_MOR_UTEN_INNHOLD;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteFarskapserklaering;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteForelder;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import no.nav.farskapsportal.backend.apps.asynkron.FarskapsportalAsynkronTestApplication;
import no.nav.farskapsportal.backend.apps.asynkron.exception.SkattConsumerException;
import no.nav.farskapsportal.backend.libs.dto.Forelderrolle;
import no.nav.farskapsportal.backend.libs.entity.Dokumentinnhold;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("SkattConsumer")
@ActiveProfiles(PROFILE_TEST)
@DirtiesContext
@SpringBootTest(classes = FarskapsportalAsynkronTestApplication.class, webEnvironment = WebEnvironment.RANDOM_PORT)
public class SkattConsumerTest {

  @Autowired
  private SkattConsumer skattConsumer;

  @Test
  void skalReturnereTidspunktForOverfoeringDersomRegistreringAvFarskapGaarIgjennomHosSkatt() {

    // given
    var farskapserklaering = henteFarskapserklaering(henteForelder(Forelderrolle.MOR), henteForelder(Forelderrolle.FAR), henteBarnUtenFnr(5));
    farskapserklaering.getDokument().getSigneringsinformasjonMor().setXadesXml("Mors signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now());
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setXadesXml("Fars signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument()
        .setDokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build());
    farskapserklaering.setMeldingsidSkatt("123");

    // when
    var tidspunktForOverfoering = skattConsumer.registrereFarskap(farskapserklaering);

    // then
    assertAll(
        () -> assertThat(tidspunktForOverfoering.isBefore(LocalDateTime.now())),
        () -> assertThat(tidspunktForOverfoering.isAfter(LocalDateTime.now().minusMinutes(5)))
    );
  }

  @Test
  void skalKasteExceptionDersomPdfDokumentetIkkeHarInnhold() {

    // given
    var farskapserklaering = henteFarskapserklaering(henteForelder(Forelderrolle.MOR), henteForelder(Forelderrolle.FAR), henteBarnUtenFnr(8));
    farskapserklaering.getDokument().getSigneringsinformasjonMor().setXadesXml("".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now());
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setXadesXml("Fars signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().setDokumentinnhold(Dokumentinnhold.builder().innhold("jadda".getBytes()).build());
    farskapserklaering.setMeldingsidSkatt("123");

    // when, then
    var skattConsumerException = assertThrows(SkattConsumerException.class, () -> skattConsumer.registrereFarskap(farskapserklaering));

    assertThat(skattConsumerException.getFeilkode()).isEqualTo(XADES_MOR_UTEN_INNHOLD);
  }

  @Test
  void skalKasteExceptionDersomMorsXadesDokumentIkkeHarInnhold() {

    // given
    var farskapserklaering = henteFarskapserklaering(henteForelder(Forelderrolle.MOR), henteForelder(Forelderrolle.FAR), henteBarnUtenFnr(8));
    farskapserklaering.getDokument().getSigneringsinformasjonMor().setXadesXml("Mors signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now());
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setXadesXml("".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().setDokumentinnhold(Dokumentinnhold.builder().innhold("jadda".getBytes()).build());
    farskapserklaering.setMeldingsidSkatt("123");

    // when, then
    var skattConsumerException = assertThrows(SkattConsumerException.class, () -> skattConsumer.registrereFarskap(farskapserklaering));

    assertThat(skattConsumerException.getFeilkode()).isEqualTo(XADES_FAR_UTEN_INNHOLD);
  }

  @Test
  void skalKasteExceptionDersomFarsXadesDokumentetIkkeHarInnhold() {

    // given
    var farskapserklaering = henteFarskapserklaering(henteForelder(Forelderrolle.MOR), henteForelder(Forelderrolle.FAR), henteBarnUtenFnr(8));
    farskapserklaering.getDokument().getSigneringsinformasjonMor().setXadesXml("Mors signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setSigneringstidspunkt(LocalDateTime.now());
    farskapserklaering.getDokument().getSigneringsinformasjonFar().setXadesXml("Fars signatur".getBytes(StandardCharsets.UTF_8));
    farskapserklaering.getDokument().setDokumentinnhold(Dokumentinnhold.builder().innhold("".getBytes()).build());
    farskapserklaering.setMeldingsidSkatt("123");

    // when, then
    var skattConsumerException = assertThrows(SkattConsumerException.class, () -> skattConsumer.registrereFarskap(farskapserklaering));

    assertThat(skattConsumerException.getFeilkode()).isEqualTo(DOKUMENT_MANGLER_INNOHLD);
  }
}
