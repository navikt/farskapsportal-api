package no.nav.farskapsportal.backend.apps.api.consumer.esignering;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_TEST;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.lageUri;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.tilUri;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import no.digipost.signature.client.core.exceptions.SenderNotSpecifiedException;
import no.digipost.signature.client.direct.DirectClient;
import no.digipost.signature.client.direct.DirectJob;
import no.digipost.signature.client.direct.ExitUrls;
import no.nav.farskapsportal.backend.apps.api.FarskapsportalApiApplicationLocal;
import no.nav.farskapsportal.backend.apps.api.FarskapsportalApiLocalConfig;
import no.nav.farskapsportal.backend.apps.api.api.Skriftspraak;
import no.nav.farskapsportal.backend.apps.api.config.egenskaper.FarskapsportalApiEgenskaper;
import no.nav.farskapsportal.backend.apps.api.consumer.esignering.stub.DifiESignaturStub;
import no.nav.farskapsportal.backend.libs.dto.ForelderDto;
import no.nav.farskapsportal.backend.libs.dto.NavnDto;
import no.nav.farskapsportal.backend.libs.entity.Dokument;
import no.nav.farskapsportal.backend.libs.entity.Dokumentinnhold;
import no.nav.farskapsportal.backend.libs.entity.Forelder;
import no.nav.farskapsportal.backend.libs.entity.Signeringsinformasjon;
import no.nav.farskapsportal.backend.libs.felles.exception.OppretteSigneringsjobbException;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("DifiESignaturConsumer")
@ExtendWith(MockitoExtension.class)
@ActiveProfiles(PROFILE_TEST)
@SpringBootTest(classes = {FarskapsportalApiApplicationLocal.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWireMock(port = 8096)
public class DifiESignaturConsumerTest {

  private static final ForelderDto MOR = ForelderDto.builder()
      .foedselsnummer("12345678910")
      .navn(NavnDto.builder().fornavn("Kjøttdeig").etternavn("Hammer").build()).build();

  private static final ForelderDto FAR = ForelderDto.builder()
      .foedselsnummer("11111122222")
      .navn(NavnDto.builder().fornavn("Rask").etternavn("Karaffel").build()).build();

  private static final String STATUS_URL = "http://localhost:8096/api/" + MOR.getFoedselsnummer() + "/direct/signature-jobs/1/status";
  private static final String PADES_URL =
      "http://localhost:8096/api/" + MOR.getFoedselsnummer() + "/direct/signature-jobs/1" + FarskapsportalApiLocalConfig.PADES;

  @Mock
  DirectClient directClientMock;

  @Autowired
  private FarskapsportalApiEgenskaper farskapsportalApiEgenskaper;

  @Autowired
  private DifiESignaturConsumer difiESignaturConsumer;

  @Autowired
  private DifiESignaturStub difiESignaturStub;

  @Nested
  @DisplayName("Teste oppretteSigneringsjobb")
  class OppretteSigneringsJobb {

    @SneakyThrows
    @Test
    @DisplayName("Skal legge til redirect url ved opprettelse av signeringsoppdrag")
    void skalLeggeTilRedirectUrlVedOpprettelseAvSigneringsoppdrag() {

      // given
      var morsRedirectUrl = "https://mors-redirect-url.no/";
      var farsRedirectUrl = "https://fars-redirect-url.no/";
      difiESignaturStub.runOppretteSigneringsjobbStub(STATUS_URL, morsRedirectUrl, farsRedirectUrl);

      var dokument = Dokument.builder()
          .navn("Farskapsportal.pdf")
          .dokumentinnhold(Dokumentinnhold.builder()
              .innhold("Farskapserklæring for barn med termindato...".getBytes(StandardCharsets.UTF_8)).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().sendtTilSignering(LocalDateTime.now()).build())
          .statusUrl("https://getstatus.no/").build();

      var mor = Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build();
      var far = Forelder.builder().foedselsnummer(FAR.getFoedselsnummer()).build();

      // when
      difiESignaturConsumer.oppretteSigneringsjobb(1, dokument, Skriftspraak.BOKMAAL, mor, far);

      // then
      assertAll(() -> assertNotNull(dokument.getSigneringsinformasjonMor().getRedirectUrl(), "Mors redirectUrl skal være lagt til"),
          () -> assertNotNull(dokument.getSigneringsinformasjonFar().getRedirectUrl(), "Far redirectUrl skal være lagt til"),
          () -> assertEquals(morsRedirectUrl, dokument.getSigneringsinformasjonMor().getRedirectUrl(), "Mors redirectUrl er riktig"),
          () -> assertEquals(farsRedirectUrl, dokument.getSigneringsinformasjonFar().getRedirectUrl(), "Fars redirectUrl er riktig"));
    }

    @Test
    void skalSetteDokumentnavnOgTittelVedOpprettelseAvSigneringsoppdragMedEngelskDokument() {

      // given
      var morsRedirectUrl = "https://mors-redirect-url.no/";
      var farsRedirectUrl = "https://fars-redirect-url.no/";
      difiESignaturStub.runOppretteSigneringsjobbStub(STATUS_URL, morsRedirectUrl, farsRedirectUrl);

      var dokument = Dokument.builder()
          .navn("Farskapsportal.pdf")
          .dokumentinnhold(Dokumentinnhold.builder()
              .innhold("Farskapserklæring for barn med termindato...".getBytes(StandardCharsets.UTF_8)).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().sendtTilSignering(LocalDateTime.now()).build())
          .statusUrl("https://getstatus.no/").build();

      var mor = Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build();
      var far = Forelder.builder().foedselsnummer(FAR.getFoedselsnummer()).build();

      // when
      difiESignaturConsumer.oppretteSigneringsjobb(1, dokument, Skriftspraak.ENGELSK, mor, far);

      // then
      assertAll(
          () -> assertThat(dokument.getSigneringsinformasjonMor().getRedirectUrl()).isNotNull(),
          () -> assertThat(dokument.getSigneringsinformasjonFar().getRedirectUrl()).isNotNull(),
          () -> assertThat(dokument.getTittel()).isEqualTo("Declaration Of Paternity"),
          () -> assertThat(dokument.getNavn()).isEqualTo("declaration-of-paternity.pdf"),
          () -> assertThat(dokument.getSigneringsinformasjonMor().getRedirectUrl()).isEqualTo(morsRedirectUrl),
          () -> assertThat(dokument.getSigneringsinformasjonFar().getRedirectUrl()).isEqualTo(farsRedirectUrl));
    }

    @Test
    void skalKasteOppretteSigneringsjobbExceptionDersomDifiklientFeiler() {

      // given
      var morsRedirectUrl = "https://mors-redirect-url.no/";
      var farsRedirectUrl = "https://fars-redirect-url.no/";
      difiESignaturStub.runOppretteSigneringsjobbStub(STATUS_URL, morsRedirectUrl, farsRedirectUrl);

      var dokument = Dokument.builder()
          .navn("Farskapsportal.pdf")
          .dokumentinnhold(Dokumentinnhold.builder()
              .innhold("Farskapserklæring for barn med termindato...".getBytes(StandardCharsets.UTF_8)).build())
          .statusUrl("https://getstatus.no/")
          .build();

      var mor = Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build();
      var far = Forelder.builder().foedselsnummer(FAR.getFoedselsnummer()).build();

      var difiEsignaturConsumerWithMocks = new DifiESignaturConsumer(farskapsportalApiEgenskaper, directClientMock);
      when(directClientMock.create(any(DirectJob.class))).thenThrow(SenderNotSpecifiedException.class);

      // when
      Assertions.assertThrows(OppretteSigneringsjobbException.class,
          () -> difiEsignaturConsumerWithMocks.oppretteSigneringsjobb(1, dokument, Skriftspraak.BOKMAAL, mor, far),
          "Skal kaste OppretteSigneringsjobbException dersom Difiklient feiler");
    }
  }

  @Nested
  @DisplayName("Teste henteStatus")
  class HenteStatus {

    // TODO: Fjerne
    @Test
    @DisplayName("Skal hente dokumentstatus etter redirect")
    @Deprecated
    void skalHenteDokumentstatusEtterRedirectPaaGamlemaaten() throws URISyntaxException {

      // given
      difiESignaturStub.runGetStatus(STATUS_URL, PADES_URL, MOR.getFoedselsnummer(), FAR.getFoedselsnummer());

      // when
      var dokumentStatusDto = difiESignaturConsumer.henteStatus("jadda",  Set.of(new URI(STATUS_URL)));

      // then
      assertAll(
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto).isNotNull(),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getStatuslenke().toString()).isEqualTo(STATUS_URL),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getPadeslenke()).isNotNull(),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getPadeslenke().toString()).isEqualTo(PADES_URL)
      );
    }

    @Test
    @DisplayName("Skal hente dokumentstatus etter redirect")
    @Deprecated
    void skalHenteDokumentstatusEtterRedirect() {

      // given
      difiESignaturStub.runGetStatus(STATUS_URL, PADES_URL, MOR.getFoedselsnummer(), FAR.getFoedselsnummer());

      // when
      var dokumentStatusDto = difiESignaturConsumer.henteStatus("jadda", UUID.randomUUID().toString(), tilUri(STATUS_URL));

      // then
      assertAll(
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto).isNotNull(),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getStatuslenke().toString()).isEqualTo(STATUS_URL),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getPadeslenke()).isNotNull(),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getPadeslenke().toString()).isEqualTo(PADES_URL),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getSignaturer().get(1).getTidspunktForStatus())
              .isAfter(ZonedDateTime.now().minusSeconds(15)),
          () -> AssertionsForClassTypes.assertThat(dokumentStatusDto.getSignaturer().get(1).getTidspunktForStatus()).isBefore(ZonedDateTime.now())
      );
    }
  }

  @Nested
  @DisplayName("Hente signert dokument")
  class HenteSignertDokument {

    //FIXME
    @Disabled("Gir UnexpectedResponse på GCP")
    @Test
    void skalHenteSignertDokumentFraPostenEsignering() throws IOException {

      // given
      ClassLoader classLoader = getClass().getClassLoader();
      var inputStream = classLoader.getResourceAsStream("__files/farskapserklaering.pdf");
      var originaltInnhold = inputStream.readAllBytes();
      difiESignaturStub.runGetSignedDocument(FarskapsportalApiLocalConfig.PADES);

      // when
      var dokumentinnhold = difiESignaturConsumer.henteSignertDokument(lageUri(FarskapsportalApiLocalConfig.PADES));

      // then
      assertArrayEquals(originaltInnhold, dokumentinnhold);
    }
  }

  @Nested
  class HenteXadesXml {

    @Test
    void skalHenteXadesXml() {

      // given
      difiESignaturStub.runGetXades(FarskapsportalApiLocalConfig.XADES);

      // when
      var dokumentStatusDto = difiESignaturConsumer.henteXadesXml(lageUri(FarskapsportalApiLocalConfig.XADES));

      // then
      assertNotNull(dokumentStatusDto);
    }
  }

  @Nested
  class HenteNyRedirectUrl {

    @Test
    void skalHenteNyRedirectUrl() throws URISyntaxException {

      // given
      var morsRedirectUrl = "https://mors-redirect-url.no/";
      difiESignaturStub.runGetNyRedirecturl(MOR.getFoedselsnummer(), DifiESignaturStub.SIGNER_URL_MOR, morsRedirectUrl);

      // when
      var dokumentStatusDto = difiESignaturConsumer.henteNyRedirectUrl(new URI(DifiESignaturStub.SIGNER_URL_MOR));

      // then
      assertNotNull(dokumentStatusDto);
    }
  }
}

