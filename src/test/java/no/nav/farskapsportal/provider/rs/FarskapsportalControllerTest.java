package no.nav.farskapsportal.provider.rs;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.FOEDSELSDATO_FAR;
import static no.nav.farskapsportal.TestUtils.FOEDSELSDATO_MOR;
import static no.nav.farskapsportal.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.TestUtils.henteFarskapserklaering;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static no.nav.farskapsportal.TestUtils.henteNyligFoedtBarn;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import no.nav.bidrag.commons.web.test.HttpHeaderTestRestTemplate;
import no.nav.farskapsportal.FarskapsportalApplicationLocal;
import no.nav.farskapsportal.api.BrukerinformasjonResponse;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.api.KontrollerePersonopplysningerRequest;
import no.nav.farskapsportal.api.OppretteFarskaperklaeringRequest;
import no.nav.farskapsportal.api.OppretteFarskapserklaeringResponse;
import no.nav.farskapsportal.api.Sivilstandtype;
import no.nav.farskapsportal.config.FarskapsportalConfig.OidcTokenSubjectExtractor;
import no.nav.farskapsportal.config.FarskapsportalEgenskaper;
import no.nav.farskapsportal.consumer.esignering.DifiESignaturConsumer;
import no.nav.farskapsportal.consumer.pdf.PdfGeneratorConsumer;
import no.nav.farskapsportal.consumer.pdl.api.FamilierelasjonRolle;
import no.nav.farskapsportal.consumer.pdl.api.FamilierelasjonerDto;
import no.nav.farskapsportal.consumer.pdl.api.KjoennType;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.consumer.pdl.stub.HentPersonFamilierelasjoner;
import no.nav.farskapsportal.consumer.pdl.stub.HentPersonFoedsel;
import no.nav.farskapsportal.consumer.pdl.stub.HentPersonKjoenn;
import no.nav.farskapsportal.consumer.pdl.stub.HentPersonNavn;
import no.nav.farskapsportal.consumer.pdl.stub.HentPersonSivilstand;
import no.nav.farskapsportal.consumer.pdl.stub.PdlApiStub;
import no.nav.farskapsportal.consumer.sts.stub.StsStub;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.DokumentDto;
import no.nav.farskapsportal.dto.DokumentStatusDto;
import no.nav.farskapsportal.dto.FarskapserklaeringDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.dto.SignaturDto;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
import no.nav.farskapsportal.service.PersistenceService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.util.UriComponentsBuilder;

@DisplayName("FarskapsportalController")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = FarskapsportalApplicationLocal.class)
@ActiveProfiles(PROFILE_TEST)
@AutoConfigureWireMock(port = 8096)
public class FarskapsportalControllerTest {

  private static final ForelderDto MOR = henteForelder(Forelderrolle.MOR);
  private static final NavnDto NAVN_MOR = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
  private static final ForelderDto FAR = henteForelder(Forelderrolle.FAR);
  private static final NavnDto NAVN_FAR = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
  private static final BarnDto BARN_UTEN_FNR = henteBarnUtenFnr(5);
  private static final BarnDto BARN_MED_FNR = henteNyligFoedtBarn();
  private static final KontrollerePersonopplysningerRequest KONTROLLEREOPPLYSNINGER_OM_FAR = KontrollerePersonopplysningerRequest.builder()
      .foedselsnummer(FAR.getFoedselsnummer()).navn(FAR.getFornavn() + " " + FAR.getEtternavn()).build();
  private static final Map<KjoennType, LocalDateTime> KJOENNSHISTORIKK_MOR = getKjoennshistorikk(KjoennType.KVINNE);
  private static final Map<KjoennType, LocalDateTime> KJOENNSHISTORIKK_FAR = getKjoennshistorikk(KjoennType.MANN);
  private static String REDIRECT_URL = "https://redirect.mot.signeringstjensesten.settes.under.normal.kjoering.etter.opprettelse.av.signeringsjobb.no";

  @LocalServerPort
  private int localServerPort;
  @Autowired
  private HttpHeaderTestRestTemplate httpHeaderTestRestTemplate;
  @Autowired
  private StsStub stsStub;
  @Autowired
  private PdlApiStub pdlApiStub;
  @MockBean
  private OidcTokenSubjectExtractor oidcTokenSubjectExtractor;
  @MockBean
  private PdfGeneratorConsumer pdfGeneratorConsumer;
  @MockBean
  private DifiESignaturConsumer difiESignaturConsumer;
  @Autowired
  private PersistenceService persistenceService;
  @Autowired
  private FarskapserklaeringDao farskapserklaeringDao;
  @Autowired
  private FarskapsportalEgenskaper farskapsportalEgenskaper;
  @Autowired
  private ModelMapper modelMapper;

  static <T> HttpEntity<T> initHttpEntity(T body, CustomHeader... customHeaders) {

    var headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    if (customHeaders != null) {
      for (var header : customHeaders) {
        headers.add(header.headerName, header.headerValue);
      }
    }

    return new HttpEntity<>(body, headers);
  }

  private static Map<KjoennType, LocalDateTime> getKjoennshistorikk(KjoennType kjoennType) {
    return Stream.of(new Object[][]{{kjoennType, LocalDateTime.now()}})
        .collect(Collectors.toMap(data -> (KjoennType) data[0], data -> (LocalDateTime) data[1]));
  }

  private String initHenteBrukerinformasjon() {
    return getBaseUrlForStubs() + "/api/v1/farskapsportal/brukerinformasjon";
  }

  private String initKontrollereOpplysningerFar() {
    return getBaseUrlForStubs() + "/api/v1/farskapsportal/personopplysninger/far";
  }

  private String initNyFarskapserklaering() {
    return getBaseUrlForStubs() + "/api/v1/farskapsportal/farskapserklaering/ny";
  }

  private String initHenteDokumentEtterRedirect() {
    return getBaseUrlForStubs() + "/api/v1/farskapsportal/farskapserklaering/redirect";
  }

  private String getBaseUrlForStubs() {
    return "http://localhost:" + localServerPort;
  }

  private void ryddeTestdata(FarskapserklaeringDto feDto) {
    var nedreGrenseTermindato = LocalDate.now().plusWeeks(farskapsportalEgenskaper.getMinAntallUkerTilTermindato());
    var oevreGrenseTermindato = LocalDate.now().plusWeeks(farskapsportalEgenskaper.getMaksAntallUkerTilTermindato());

    var fes = farskapserklaeringDao
        .henteFarskapserklaeringer(feDto.getMor().getFoedselsnummer(), feDto.getFar().getFoedselsnummer(), nedreGrenseTermindato,
            oevreGrenseTermindato);
    if (!fes.isEmpty()) {
      for (Farskapserklaering fe : fes) {
        farskapserklaeringDao.delete(fe);
      }
    }
  }

  @Test
  @DisplayName("SkaLagreOppdatertPadesUrlVedHentingAvDokument")
  void skalLagreOppdatertPadesUrlVedHentingAvDokument() throws URISyntaxException {

    // given
    var farskapserklaeringSignertAvMor = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
    farskapserklaeringSignertAvMor.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(10));

    // Slette dersom allerede eksisterer i databasen
    ryddeTestdata(farskapserklaeringSignertAvMor);

    var farskapserklaering = modelMapper.map(farskapserklaeringSignertAvMor, Farskapserklaering.class);
    var lagretFarskapserklaeringSignertAvMor = farskapserklaeringDao.save(farskapserklaering);

    var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
    var statuslenke = lagretFarskapserklaeringSignertAvMor.getDokument().getDokumentStatusUrl();
    when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(FAR.getFoedselsnummer());
    stsStub.runSecurityTokenServiceStub("jalla");
    Map<KjoennType, LocalDateTime> kjoennshistorikkFar = getKjoennshistorikk(KjoennType.MANN);

    pdlApiStub.runPdlApiHentPersonStub(
        List.of(new HentPersonKjoenn(kjoennshistorikkFar), new HentPersonFoedsel(FOEDSELSDATO_FAR, false), new HentPersonNavn(registrertNavnFar)),
        FAR.getFoedselsnummer());

    when(difiESignaturConsumer.henteDokumentstatusEtterRedirect(any(), any())).thenReturn(
        DokumentStatusDto.builder().statuslenke(statuslenke).erSigneringsjobbenFerdig(true).padeslenke(new URI("https://permanent-pades-url.no/"))
            .signaturer(List.of(SignaturDto.builder().signatureier(FAR.getFoedselsnummer()).harSignert(true)
                .tidspunktForSignering(LocalDateTime.now().minusSeconds(3)).build())).build());

    when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(lagretFarskapserklaeringSignertAvMor.getDokument().getInnhold());

    // when
    var respons = httpHeaderTestRestTemplate.exchange(
        UriComponentsBuilder.fromHttpUrl(initHenteDokumentEtterRedirect()).queryParam("status_query_token", "Sjalalala-lala").build().encode()
            .toString(), HttpMethod.PUT, null, byte[].class);

    // then
    assertTrue(respons.getStatusCode().is2xxSuccessful());

  }

  private void brukeStandardMocks(String fnrPaaloggetBruker) {
    var sivilstandMor = Sivilstandtype.UGIFT;

    brukeStandardMocksUtenPdlApi(fnrPaaloggetBruker);
    brukeStandardMocksPdlApiAngiSivilstandMor(sivilstandMor);
  }

  private void brukeStandardMocksPdlApiAngiSivilstandMor(Sivilstandtype sivilstandMor) {
    pdlApiStub.runPdlApiHentPersonStub(
        List.of(new HentPersonKjoenn(KJOENNSHISTORIKK_MOR), new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonSivilstand(sivilstandMor),
            new HentPersonNavn(NAVN_MOR)), MOR.getFoedselsnummer());

    pdlApiStub.runPdlApiHentPersonStub(
        List.of(new HentPersonKjoenn(KJOENNSHISTORIKK_FAR), new HentPersonFoedsel(FOEDSELSDATO_FAR, false), new HentPersonNavn(NAVN_FAR)),
        KONTROLLEREOPPLYSNINGER_OM_FAR.getFoedselsnummer());

  }

  private void brukeStandardMocksUtenPdlApi(String fnrPaaloggetBruker) {

    stsStub.runSecurityTokenServiceStub("jalla");

    when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(fnrPaaloggetBruker);
    var redirectUrlMor = URI.create(REDIRECT_URL);

    var pdf = DokumentDto.builder().dokumentnavn("Farskapserklæering.pdf").innhold("Jeg erklærer med dette farskap til barnet..".getBytes())
        .redirectUrlMor(redirectUrlMor).build();

    when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
    doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());
  }

  private URI getRedirectUrlMor() {
    return URI.create(REDIRECT_URL);
  }

  private static class CustomHeader {

    String headerName;
    String headerValue;

    CustomHeader(String headerName, String headerValue) {
      this.headerName = headerName;
      this.headerValue = headerValue;
    }
  }

  @Nested
  @DisplayName("Teste henteBrukerinformasjon")
  class HenteBrukerinformasjon {

    @Test
    @DisplayName("Skal liste nylig fødte barn uten registrert far ved henting av brukerinformasjon for mor")
    void skalListeNyligFoedteBarnUtenRegistrertFarVedHentingAvBrukerinformasjonForMor() {

      // given
      var foedselsdatoSpedbarn = LocalDate.now().minusMonths(2).minusDays(13);
      var fnrSpedbarn = foedselsdatoSpedbarn.format(DateTimeFormatter.ofPattern("ddMMyy")) + "00000";
      var foedselsdatoMor = foedselsdatoSpedbarn.minusYears(28).minusMonths(2).minusDays(13);
      var fnrMor = foedselsdatoMor.format(DateTimeFormatter.ofPattern("ddMMyy")) + "12340";

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.KVINNE);
      stsStub.runSecurityTokenServiceStub("jalla");
      var morsRelasjonTilBarn = FamilierelasjonerDto.builder().minRolleForPerson(FamilierelasjonRolle.MOR)
          .relatertPersonsRolle(FamilierelasjonRolle.BARN).relatertPersonsIdent(fnrSpedbarn).build();
      var spedbarnetsRelasjonTilMor = FamilierelasjonerDto.builder().relatertPersonsRolle(FamilierelasjonRolle.MOR).relatertPersonsIdent(fnrMor)
          .minRolleForPerson(FamilierelasjonRolle.BARN).build();

      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonFamilierelasjoner(morsRelasjonTilBarn, "123"), new HentPersonSivilstand(Sivilstandtype.UGIFT),
              new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonKjoenn(kjoennshistorikk)), fnrMor);
      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonFamilierelasjoner(spedbarnetsRelasjonTilMor, "000"), new HentPersonFoedsel(foedselsdatoSpedbarn, false)),
          fnrSpedbarn);
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(fnrMor);

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(HttpStatus.OK.value(), respons.getStatusCode().value()),
          () -> assertEquals(Forelderrolle.MOR, brukerinformasjonResponse.getForelderrolle(), "Mor skal ha forelderrolle MOR"),
          () -> assertEquals(1, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().size(),
              "Lista over nyfødte barn uten registrert far skal inneholde ett element"),
          () -> assertEquals(fnrSpedbarn, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().iterator().next(),
              "Spedbarnet i lista over nyfødte barn uten registrert far skal ha riktig fødselsnummer"));
    }

    @Test
    @DisplayName("Skal liste farskapserklæringer som venter på fars signatur ved henting av brukerinformasjon for mor")
    void skalListeFarskapserklaeringerSomVenterPaaFarVedHentingAvBrukerinformasjonForMor() {

      // given
      var farskapserklaeringSomVenterPaaFar = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      farskapserklaeringSomVenterPaaFar.getDokument().setSignertAvMor(LocalDateTime.now().minusDays(3));

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringSomVenterPaaFar);

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFar);

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.KVINNE);

      stsStub.runSecurityTokenServiceStub("jalla");

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonFamilierelasjoner(null, null), new HentPersonSivilstand(Sivilstandtype.UGIFT),
          new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonKjoenn(kjoennshistorikk)), MOR.getFoedselsnummer());
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(MOR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(HttpStatus.OK.value(), respons.getStatusCode().value()),
          () -> assertEquals(Forelderrolle.MOR, brukerinformasjonResponse.getForelderrolle(), "Mor skal ha forelderrolle MOR"),
          () -> assertEquals(1, brukerinformasjonResponse.getAvventerSigneringMotpart().size(),
              "Det er en farskapserklæring som venter på fars signatur"),
          () -> assertNull(brukerinformasjonResponse.getAvventerSigneringMotpart().iterator().next().getDokument().getSignertAvFar(),
              "Far har ikke signert farskapserklæringen"), () -> assertEquals(FAR.getFoedselsnummer(),
              brukerinformasjonResponse.getAvventerSigneringMotpart().iterator().next().getFar().getFoedselsnummer(),
              "Farskapserklæringen gjelder riktig far"),
          () -> assertEquals(0, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().size()),
          () -> assertEquals(0, brukerinformasjonResponse.getAvventerSigneringBruker().size()));

      // cleanup db
      farskapserklaeringDao.delete(lagretFarskapserklaering);
    }

    @Test
    @DisplayName("Skal liste farskapserklæringer som venter på mors signatur ved henting av brukerinformasjon for mor")
    void skalListeFarskapserklaeringerSomVenterPaaMorVedHentingAvBrukerinformasjonForMor() {

      // given
      var farskapserklaeringSomVenterPaaMor = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      farskapserklaeringSomVenterPaaMor.getDokument().setPadesUrl(null);

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringSomVenterPaaMor);

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaMor);

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.KVINNE);

      stsStub.runSecurityTokenServiceStub("jalla");

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonFamilierelasjoner(null, null), new HentPersonSivilstand(Sivilstandtype.UGIFT),
          new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonKjoenn(kjoennshistorikk)), MOR.getFoedselsnummer());
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(MOR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(HttpStatus.OK.value(), respons.getStatusCode().value()),
          () -> assertEquals(Forelderrolle.MOR, brukerinformasjonResponse.getForelderrolle(), "Mor skal ha forelderrolle MOR"),
          () -> assertEquals(1, brukerinformasjonResponse.getAvventerSigneringBruker().size()),
          () -> assertNull(brukerinformasjonResponse.getAvventerSigneringBruker().iterator().next().getDokument().getSignertAvMor()),
          () -> assertEquals(FAR.getFoedselsnummer(),
              brukerinformasjonResponse.getAvventerSigneringBruker().iterator().next().getFar().getFoedselsnummer()),
          () -> assertEquals(0, brukerinformasjonResponse.getAvventerSigneringMotpart().size()),
          () -> assertEquals(0, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().size()));

      // cleanup db
      farskapserklaeringDao.delete(lagretFarskapserklaering);
    }

    @Test
    @DisplayName("Skal liste farskapserklæringer som venter på far ved henting av brukerinformasjon for far")
    void skalListeFarskapserklaeringerSomVenterPaaFarVedHentingAvBrukerinformasjonForFar() {

      // given
      var farskapserklaeringSomVenterPaaFar = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      farskapserklaeringSomVenterPaaFar.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(10));

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringSomVenterPaaFar);

      persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFar);

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.MANN);

      stsStub.runSecurityTokenServiceStub("jalla");

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonFamilierelasjoner(null, null), new HentPersonSivilstand(Sivilstandtype.UGIFT),
          new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonKjoenn(kjoennshistorikk)), FAR.getFoedselsnummer());
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(FAR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(HttpStatus.OK.value(), respons.getStatusCodeValue()),
          () -> assertEquals(Forelderrolle.FAR, brukerinformasjonResponse.getForelderrolle(), "Far skal ha forelderrolle FAR"),
          () -> assertEquals(0, brukerinformasjonResponse.getAvventerSigneringMotpart().size(),
              "Det er en farskapserklæring som venter på fars signatur"),
          () -> assertEquals(1, brukerinformasjonResponse.getAvventerSigneringBruker().size()), () -> assertEquals(FAR.getFoedselsnummer(),
              brukerinformasjonResponse.getAvventerSigneringBruker().iterator().next().getFar().getFoedselsnummer(),
              "Farskapserklæringen gjelder riktig far"),
          () -> assertEquals(0, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().size()));

      // cleanup db
      farskapserklaeringDao.deleteAll();
    }

    @Test
    @DisplayName("Farskapserklæringer som venter på mor skal ikke dukke opp i fars liste")
    void skalIkkeListeFarskapserklaeringerSomVenterPaaMorVedHentingAvBrukerinformasjonForFar() {

      // given
      var farskapserklaeringSomVenterPaaMor = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      farskapserklaeringSomVenterPaaMor.getDokument().setPadesUrl(null);
      farskapserklaeringSomVenterPaaMor.getDokument().setSignertAvMor(null);
      farskapserklaeringSomVenterPaaMor.getDokument().setSignertAvFar(null);

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringSomVenterPaaMor);

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaMor);

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.MANN);

      stsStub.runSecurityTokenServiceStub("jalla");

      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonFamilierelasjoner(null, null), new HentPersonKjoenn(kjoennshistorikk), new HentPersonFoedsel(FOEDSELSDATO_MOR, false),
              new HentPersonSivilstand(Sivilstandtype.UGIFT)), FAR.getFoedselsnummer());

      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(FAR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(0, brukerinformasjonResponse.getAvventerSigneringBruker().size()),
          () -> assertEquals(0, brukerinformasjonResponse.getAvventerSigneringMotpart().size()),
          () -> assertEquals(0, brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar().size()));

      // cleanup db
      farskapserklaeringDao.delete(lagretFarskapserklaering);
    }

    @Test
    @DisplayName("Skal gi not found dersom person ikke eksisterer")
    void skalGiNotFoundDersomPersonIkkeEksisterer() {

      // given
      var foedselsdatoSpedbarn = LocalDate.now().minusMonths(2).minusDays(13);
      var fnrSpedbarn = foedselsdatoSpedbarn.format(DateTimeFormatter.ofPattern("ddMMyy")) + "00000";
      var foedselsdatoMor = foedselsdatoSpedbarn.minusYears(28).minusMonths(2).minusDays(13);
      var fnrMor = foedselsdatoMor.format(DateTimeFormatter.ofPattern("ddMMyy")) + "12340";

      var kjoennshistorikk = new HashMap<KjoennType, LocalDateTime>();
      stsStub.runSecurityTokenServiceStub("jalla");

      var spedbarnetsRelasjonTilMor = FamilierelasjonerDto.builder().relatertPersonsRolle(FamilierelasjonRolle.MOR).relatertPersonsIdent(fnrMor)
          .minRolleForPerson(FamilierelasjonRolle.BARN).build();

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonFamilierelasjoner(null, ""), new HentPersonKjoenn(kjoennshistorikk)), fnrMor);
      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonFamilierelasjoner(spedbarnetsRelasjonTilMor, "000"), new HentPersonFoedsel(foedselsdatoSpedbarn, false)),
          fnrSpedbarn);
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(fnrMor);

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertEquals(HttpStatus.NOT_FOUND.value(), respons.getStatusCode().value()),
          () -> assertFalse(brukerinformasjonResponse.isKanOppretteFarskapserklaering()),
          () -> assertNull(brukerinformasjonResponse.getForelderrolle()), () -> assertNull(brukerinformasjonResponse.getAvventerSigneringMotpart()),
          () -> assertNull(brukerinformasjonResponse.getAvventerSigneringBruker()),
          () -> assertNull(brukerinformasjonResponse.getFnrNyligFoedteBarnUtenRegistrertFar()));
    }

    @Test
    @DisplayName("Far skal se liste over signerte farskapserklæringer som venter på registrering hos Skatt")
    void farSkalSeListeOverSignerteFarskapserklaeringerSomVenterPaaRegistrering() {

      // given
      farskapserklaeringDao.deleteAll();
      brukeStandardMocks(FAR.getFoedselsnummer());

      var signertFarskapserklaering = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      signertFarskapserklaering.getDokument().setSignertAvMor(LocalDateTime.now());
      signertFarskapserklaering.getDokument().setSignertAvFar(LocalDateTime.now());

      persistenceService.lagreFarskapserklaering(signertFarskapserklaering);

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initHenteBrukerinformasjon(), HttpMethod.GET, null, BrukerinformasjonResponse.class);

      var brukerinformasjonResponse = respons.getBody();

      // then
      assertAll(() -> assertTrue(respons.getStatusCode().is2xxSuccessful()),
          () -> assertFalse(brukerinformasjonResponse.isKanOppretteFarskapserklaering()),
          () -> assertEquals(Forelderrolle.FAR, brukerinformasjonResponse.getForelderrolle()),
          () -> assertEquals(1, brukerinformasjonResponse.getAvventerRegistrering().size()));
    }

    @Test
    @DisplayName("Bruker med forelderrolle MOR_ELLER_FAR skal se ventende farskapserklæringer for både mor og far")
    void brukerMedForelderrolleMorEllerFarSkalSeVentendeFarskapserklaeringerForBaadeMorOgFar() {
      farskapserklaeringDao.deleteAll();
      brukeStandardMocks(FAR.getFoedselsnummer());
    }
  }

  @Nested
  @DisplayName("Teste kontrollereOpplysningerFar")
  class KontrollereOpplysningerFar {

    @Test
    @DisplayName("Skal gi Ok dersom navn og kjønn er riktig")
    void skalGiOkDersomNavnOgKjoennErRiktig() {

      // given
      var registrertNavn = NavnDto.builder().fornavn("Borat").etternavn("Sagdiyev").build();
      stsStub.runSecurityTokenServiceStub("jalla");

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.MANN);

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonKjoenn(kjoennshistorikk), new HentPersonNavn(registrertNavn)));

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initKontrollereOpplysningerFar(), HttpMethod.POST,
          initHttpEntity(KontrollerePersonopplysningerRequest.builder().foedselsnummer("01057244444").navn("Borat Sagdiyev").build()),
          HttpStatus.class);

      // then
      assertTrue(respons.getStatusCode().is2xxSuccessful());
    }

    @Test
    @DisplayName("Skal gi bad request dersom oppgitt far er kvinne")
    void skalGiBadRequestDersomOppgittFarErKvinne() {

      // given
      var oppgittNavn = NavnDto.builder().fornavn("Natalya").etternavn("Sagdiyev").build();
      stsStub.runSecurityTokenServiceStub("jalla");

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.KVINNE);

      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonKjoenn(kjoennshistorikk), new HentPersonNavn(oppgittNavn), new HentPersonFoedsel(FOEDSELSDATO_MOR, false)));

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initKontrollereOpplysningerFar(), HttpMethod.POST,
          initHttpEntity(KontrollerePersonopplysningerRequest.builder().foedselsnummer("01058011444").navn("Natalya Sagdiyev").build()),
          String.class);

      // then
      assertTrue(respons.getStatusCode().is4xxClientError());
    }

    @Test
    @DisplayName("Skal gi bad request dersom navn er gjengitt feil i spørring")
    void skalGiBadRequestDersomNavnErGjengittFeilISpoerring() {

      // given
      var registrertNavn = NavnDto.builder().fornavn("Borat").etternavn("Sagdiyev").build();
      stsStub.runSecurityTokenServiceStub("jalla");

      Map<KjoennType, LocalDateTime> kjoennshistorikk = getKjoennshistorikk(KjoennType.MANN);

      pdlApiStub.runPdlApiHentPersonStub(List.of(new HentPersonKjoenn(kjoennshistorikk), new HentPersonNavn(registrertNavn)));

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initKontrollereOpplysningerFar(), HttpMethod.POST,
          initHttpEntity(KontrollerePersonopplysningerRequest.builder().foedselsnummer("01058011444").navn("Borat Nicolai Sagdiyev").build()),
          String.class);

      // then
      assertTrue(respons.getStatusCode().is4xxClientError());
    }

    @Test
    @DisplayName("Skal gi not found dersom person ikke eksisterer i PDL")
    void skalGiNotFoundDersomPersonIkkeEksistererIPdl() {

      // given
      stsStub.runSecurityTokenServiceStub("jalla");
      pdlApiStub.runPdlApiHentPersonFantIkkePersonenStub();

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initKontrollereOpplysningerFar(), HttpMethod.POST,
          initHttpEntity(KontrollerePersonopplysningerRequest.builder().foedselsnummer("01058011444").navn("Borat Sagdiyev").build()), String.class);

      // then
      assertSame(respons.getStatusCode(), HttpStatus.NOT_FOUND);
    }
  }

  @Nested
  @DisplayName("Teste nyFarskapserklaering")
  class NyFarskapserklaering {

    @Test
    @DisplayName("Mor skal kunne opprette farskapserklaering for barn med termindato")
    void morSkalKunneOppretteFarskapserklaeringForBarnMedTermindato() {

      // given
      farskapserklaeringDao.deleteAll();
      brukeStandardMocks(MOR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST,
          initHttpEntity(OppretteFarskaperklaeringRequest.builder().barn(BARN_UTEN_FNR).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR).build()),
          OppretteFarskapserklaeringResponse.class);

      // then
      assertAll(() -> assertTrue(respons.getStatusCode().is2xxSuccessful()),
          () -> assertEquals(getRedirectUrlMor(), respons.getBody().getRedirectUrlForSigneringMor()));

      // rydde testdata
      farskapserklaeringDao.deleteAll();
    }

    @Test
    @DisplayName("Skal returnere feilkode dersom farskapserklæring allerede eksisterer for utfødt barn med samme foreldre")
    void skalReturnereFeilkodeDersomFarskapserklaeringAlleredeEksistererForUfoedtBarnMedSammeForeldre() {

      // given
      farskapserklaeringDao.deleteAll();

      var farskapserklaeringSomVenterPaaFar = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);
      farskapserklaeringSomVenterPaaFar.getDokument().setSignertAvMor(LocalDateTime.now().minusDays(3));
      var eksisterendeFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFar);

      brukeStandardMocks(MOR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST,
          initHttpEntity(OppretteFarskaperklaeringRequest.builder().barn(BARN_UTEN_FNR).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR).build()),
          OppretteFarskapserklaeringResponse.class);

      // then
      assertEquals(HttpStatus.BAD_REQUEST.value(), respons.getStatusCodeValue());

      // cleanup db
      farskapserklaeringDao.delete(eksisterendeFarskapserklaering);
    }

    @Test
    @DisplayName("Skal gi BAD REQUEST dersom termindato er utenfor gyldig område")
    void skalGiBadRequestDersomTermindatoErUtenforGyldigOmraade() {

      // given
      brukeStandardMocks(MOR.getFoedselsnummer());

      var barnMedTermindatoForLangtFremITid = BarnDto.builder()
          .termindato(LocalDate.now().plusWeeks(farskapsportalEgenskaper.getMaksAntallUkerTilTermindato() + 1)).build();

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST, initHttpEntity(
          OppretteFarskaperklaeringRequest.builder().barn(barnMedTermindatoForLangtFremITid).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR)
              .build()), OppretteFarskapserklaeringResponse.class);

      // then
      assertEquals(HttpStatus.BAD_REQUEST.value(), respons.getStatusCodeValue());
    }

    @Test
    @DisplayName("Skal gi BAD request dersom oppgitt nyfoedt mangler relasjon til mor")
    void skalGiBadRequestDersomOppgittNyfoedtManglerRelasjonTilMor() {

      // given
      brukeStandardMocks(MOR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST,
          initHttpEntity(OppretteFarskaperklaeringRequest.builder().barn(BARN_MED_FNR).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR).build()),
          OppretteFarskapserklaeringResponse.class);

      // then
      assertEquals(HttpStatus.BAD_REQUEST.value(), respons.getStatusCodeValue());
    }

    @Test
    @DisplayName("Mor kan opprette farskapserklæring selv om far er gift")
    void morKanOppretteFarskapserklaeringSelvOmFarErGift() {

      // given
      brukeStandardMocksUtenPdlApi(MOR.getFoedselsnummer());

      var sivilstandMor = Sivilstandtype.UGIFT;
      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonKjoenn(KJOENNSHISTORIKK_MOR), new HentPersonFoedsel(FOEDSELSDATO_MOR, false), new HentPersonSivilstand(sivilstandMor),
              new HentPersonNavn(NAVN_MOR)), MOR.getFoedselsnummer());

      var sivilstandFar = Sivilstandtype.GIFT;
      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonKjoenn(KJOENNSHISTORIKK_FAR), new HentPersonFoedsel(FOEDSELSDATO_FAR, false), new HentPersonNavn(NAVN_FAR),
              new HentPersonSivilstand(sivilstandFar)), KONTROLLEREOPPLYSNINGER_OM_FAR.getFoedselsnummer());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST,
          initHttpEntity(OppretteFarskaperklaeringRequest.builder().barn(BARN_UTEN_FNR).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR).build()),
          OppretteFarskapserklaeringResponse.class);

      // then
      assertTrue(respons.getStatusCode().is2xxSuccessful());
    }

    @Test
    @DisplayName("Opprettelse av farskapserklæring skal gi BAD REQUEST dersom mor er gift")
    void opprettelseAvFarskapserklaeringSkalGiBadRequestDersomMorErGift() {

      // given
      var sivilstandMor = Sivilstandtype.GIFT;
      brukeStandardMocksUtenPdlApi(MOR.getFoedselsnummer());
      brukeStandardMocksPdlApiAngiSivilstandMor(sivilstandMor);

      // when
      var respons = httpHeaderTestRestTemplate.exchange(initNyFarskapserklaering(), HttpMethod.POST,
          initHttpEntity(OppretteFarskaperklaeringRequest.builder().barn(BARN_MED_FNR).opplysningerOmFar(KONTROLLEREOPPLYSNINGER_OM_FAR).build()),
          OppretteFarskapserklaeringResponse.class);

      // then
      assertEquals(HttpStatus.BAD_REQUEST.value(), respons.getStatusCodeValue());
    }
  }

  @Nested
  @DisplayName("Teste henteDokumentEtterRedirect")
  class HenteDokumentEtterRedirect {

    @SneakyThrows
    @Test
    @DisplayName("Skal hente signert dokument for mor etter redirect")
    void skalHenteSignertDokumentForMorEtterRedirect() {

      // given
      var farskapserklaeringUtenSignaturer = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringUtenSignaturer);

      farskapserklaeringUtenSignaturer.getDokument().setPadesUrl(null);

      assertAll(() -> assertNull(farskapserklaeringUtenSignaturer.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringUtenSignaturer.getDokument().getSignertAvFar()),
          () -> assertNull(farskapserklaeringUtenSignaturer.getDokument().getPadesUrl()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringUtenSignaturer);

      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var statuslenke = lagretFarskapserklaering.getDokument().getDokumentStatusUrl();
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(MOR.getFoedselsnummer());
      stsStub.runSecurityTokenServiceStub("jalla");
      Map<KjoennType, LocalDateTime> kjoennshistorikkMor = getKjoennshistorikk(KjoennType.KVINNE);

      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonKjoenn(kjoennshistorikkMor), new HentPersonNavn(registrertNavnMor), new HentPersonFoedsel(FOEDSELSDATO_FAR, false)),
          MOR.getFoedselsnummer());

      when(difiESignaturConsumer.henteDokumentstatusEtterRedirect(any(), any())).thenReturn(
          DokumentStatusDto.builder().statuslenke(statuslenke).erSigneringsjobbenFerdig(true).padeslenke(new URI("https://permanent-pades-url.no/"))
              .signaturer(List.of(SignaturDto.builder().signatureier(MOR.getFoedselsnummer()).harSignert(true)
                  .tidspunktForSignering(LocalDateTime.now().minusSeconds(3)).build())).build());

      when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(lagretFarskapserklaering.getDokument().getInnhold());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(
          UriComponentsBuilder.fromHttpUrl(initHenteDokumentEtterRedirect()).queryParam("status_query_token", "Sjalalala-lala").build().encode()
              .toString(), HttpMethod.PUT, null, byte[].class);

      // then
      assertTrue(respons.getStatusCode().is2xxSuccessful());

      // clean-up testdata
      farskapserklaeringDao.delete(lagretFarskapserklaering);
    }

    @SneakyThrows
    @Test
    @DisplayName("Skal hente signert dokument for far etter redirect")
    void skalHenteSignertDokumentForFarEtterRedirect() {

      // given
      var farskapserklaeringSignertAvMor = henteFarskapserklaering(MOR, FAR, BARN_UTEN_FNR);

      farskapserklaeringSignertAvMor.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(10));

      // Slette dersom allerede eksisterer i databasen
      ryddeTestdata(farskapserklaeringSignertAvMor);

      var lagretFarskapserklaeringSignertAvMor = persistenceService.lagreFarskapserklaering(farskapserklaeringSignertAvMor);

      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var statuslenke = lagretFarskapserklaeringSignertAvMor.getDokument().getDokumentStatusUrl();
      when(oidcTokenSubjectExtractor.hentPaaloggetPerson()).thenReturn(FAR.getFoedselsnummer());
      stsStub.runSecurityTokenServiceStub("jalla");
      Map<KjoennType, LocalDateTime> kjoennshistorikkFar = getKjoennshistorikk(KjoennType.MANN);

      pdlApiStub.runPdlApiHentPersonStub(
          List.of(new HentPersonKjoenn(kjoennshistorikkFar), new HentPersonNavn(registrertNavnFar), new HentPersonFoedsel(FOEDSELSDATO_FAR, false)),
          FAR.getFoedselsnummer());

      when(difiESignaturConsumer.henteDokumentstatusEtterRedirect(any(), any())).thenReturn(
          DokumentStatusDto.builder().statuslenke(statuslenke).erSigneringsjobbenFerdig(true).padeslenke(new URI("https://permanent-pades-url.no/"))
              .signaturer(List.of(SignaturDto.builder().signatureier(FAR.getFoedselsnummer()).harSignert(true)
                  .tidspunktForSignering(LocalDateTime.now().minusSeconds(3)).build())).build());

      when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(lagretFarskapserklaeringSignertAvMor.getDokument().getInnhold());

      // when
      var respons = httpHeaderTestRestTemplate.exchange(
          UriComponentsBuilder.fromHttpUrl(initHenteDokumentEtterRedirect()).queryParam("status_query_token", "Sjalalala-lala").build().encode()
              .toString(), HttpMethod.PUT, null, byte[].class);

      // then
      assertTrue(respons.getStatusCode().is2xxSuccessful());

      // clean-up testdata
      farskapserklaeringDao.delete(lagretFarskapserklaeringSignertAvMor);
    }
  }
}
