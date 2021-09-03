package no.nav.farskapsportal.consumer.pdf;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static no.nav.farskapsportal.TestUtils.henteNyligFoedtBarn;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.api.Skriftspraak;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.ForelderDto;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("PdfGeneratorConsumerTest")
@SpringBootTest(classes = PdfGeneratorConsumer.class)
@ActiveProfiles(PROFILE_TEST)
public class PdfGeneratorConsumerTest {

  private static final ForelderDto MOR = henteForelder(Forelderrolle.MOR);
  private static final ForelderDto FAR = henteForelder(Forelderrolle.FAR);
  private static final BarnDto UFOEDT_BARN = henteBarnUtenFnr(17);
  private static final BarnDto NYFOEDT_BARN = henteNyligFoedtBarn();
  private static boolean skriveUtPdf = false;

  @Autowired
  private PdfGeneratorConsumer pdfGeneratorConsumer;

  @Test
  void skalGenererePdfPaaBokmaalForUfoedt() throws IOException {

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(UFOEDT_BARN, MOR, FAR, Optional.empty());

    // then
    if (skriveUtPdf) {
      skriveUtPdfForInspeksjon(pdfstroem);
    }

    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Fødested"),
        () -> assertThat(dokumenttekst).contains("Termindato: " + UFOEDT_BARN.getTermindato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))
    );

    validereInformasjonOmForeldrenePaaBokmaal(dokumenttekst, MOR, FAR);

  }

  @Test
  void skalGenererePdfPaaEngelskForUfoedt() throws IOException {

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(UFOEDT_BARN, MOR, FAR, Optional.of(Skriftspraak.ENGELSK));

    // then
    if (skriveUtPdf) {
      skriveUtPdfForInspeksjon(pdfstroem);
    }

    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Place of birth"),
        () -> assertThat(dokumenttekst).contains("Expected date of birth: " + UFOEDT_BARN.getTermindato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))
    );

    validereInformasjonOmForeldrenePaaEngelsk(dokumenttekst, MOR, FAR);
  }

  @Test
  void skalGenererePdfPaaBokmaalForNyfoedt() throws IOException {

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(NYFOEDT_BARN, MOR, FAR, Optional.empty());

    // then
    if (skriveUtPdf) {
      skriveUtPdfForInspeksjon(pdfstroem);
    }

    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Termindato"),
        () -> assertThat(dokumenttekst).contains("Opplysninger om barnet"),
        () -> assertThat(dokumenttekst).contains("Fødselsnummer: " + NYFOEDT_BARN.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Fødselsdato: " + NYFOEDT_BARN.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Fødested: " + NYFOEDT_BARN.getFoedested())
    );

    validereInformasjonOmForeldrenePaaBokmaal(dokumenttekst, MOR, FAR);
  }

  @Test
  void skalGenererePdfPaaEngelskForNyfoedt() throws IOException {

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(NYFOEDT_BARN, MOR, FAR, Optional.of(Skriftspraak.ENGELSK));

    // then
    if (skriveUtPdf) {
      skriveUtPdfForInspeksjon(pdfstroem);
    }

    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Expected date of birth "),
        () -> assertThat(dokumenttekst).contains("Child"),
        () -> assertThat(dokumenttekst).contains("Social security number: " + NYFOEDT_BARN.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Date of birth: " + NYFOEDT_BARN.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Place of birth: " + NYFOEDT_BARN.getFoedested())
    );

    validereInformasjonOmForeldrenePaaEngelsk(dokumenttekst, MOR, FAR);
  }

  @Test
  void skalGenererePdfPaaBokmaalForForelderMedMellomnavn() throws IOException {

    // given
    var farMedMellomnavn = henteForelder(Forelderrolle.FAR);
    var farsOpprinneligeNavn = farMedMellomnavn.getNavn();

    farMedMellomnavn.setNavn(
        NavnDto.builder()
            .fornavn(farsOpprinneligeNavn.getFornavn())
            .mellomnavn("Strømstad")
            .etternavn(farsOpprinneligeNavn.getEtternavn()).build());

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(NYFOEDT_BARN, MOR, farMedMellomnavn, Optional.empty());

    // then
    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Termindato"),
        () -> assertThat(dokumenttekst).contains("Opplysninger om barnet"),
        () -> assertThat(dokumenttekst).contains("Fødselsnummer: " + NYFOEDT_BARN.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Fødselsdato: " + NYFOEDT_BARN.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Fødested: " + NYFOEDT_BARN.getFoedested())
    );

    validereInformasjonOmForeldrenePaaBokmaal(dokumenttekst, MOR, farMedMellomnavn);
  }

  @Test
  void skalGenererePdfPaaEngelskForForelderMedMellomnavn() throws IOException {

    // given
    var farMedMellomnavn = henteForelder(Forelderrolle.FAR);
    var farsOpprinneligeNavn = farMedMellomnavn.getNavn();

    farMedMellomnavn.setNavn(
        NavnDto.builder()
            .fornavn(farsOpprinneligeNavn.getFornavn())
            .mellomnavn("Strømstad")
            .etternavn(farsOpprinneligeNavn.getEtternavn()).build());

    // when
    var pdfstroem = pdfGeneratorConsumer.genererePdf(NYFOEDT_BARN, MOR, farMedMellomnavn, Optional.of(Skriftspraak.ENGELSK));

    // then
    PDDocument doc = PDDocument.load(pdfstroem);
    PDFTextStripper pdfTextStripper = new PDFTextStripper();
    String dokumenttekst = pdfTextStripper.getText(doc);

    if (skriveUtPdf) {
      skriveUtPdfForInspeksjon(pdfstroem);
    }

    assertAll(
        () -> assertThat(dokumenttekst).doesNotContain("Expected date of birth "),
        () -> assertThat(dokumenttekst).contains("Child"),
        () -> assertThat(dokumenttekst).contains("Social security number: " + NYFOEDT_BARN.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Date of birth: " + NYFOEDT_BARN.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Place of birth: " + NYFOEDT_BARN.getFoedested())
    );

    validereInformasjonOmForeldrenePaaEngelsk(dokumenttekst, MOR, farMedMellomnavn);
  }


  private void skriveUtPdfForInspeksjon(byte[] pdfstroem) {
    try (final FileOutputStream filstroem = new FileOutputStream("farskapserklaering.pdf")) {
      filstroem.write(pdfstroem);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }



  private void validereInformasjonOmForeldrenePaaBokmaal(String dokumenttekst, ForelderDto mor, ForelderDto far) {

    var navnFar = far.getNavn().sammensattNavn();

    var navnMor = mor.getNavn().sammensattNavn();

    assertAll(
        () -> assertThat(dokumenttekst).contains("Opplysninger om mor"),
        () -> assertThat(dokumenttekst).contains("Fødselsnummer: " + mor.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Fødselsdato: " + mor.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Navn: " + navnMor),
        () -> assertThat(dokumenttekst).contains("Opplysninger om far"),
        () -> assertThat(dokumenttekst).contains("Fødselsnummer: " + far.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Fødselsdato: " + far.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Navn: " + navnFar)
    );
  }

  private void validereInformasjonOmForeldrenePaaEngelsk(String dokumenttekst, ForelderDto mor, ForelderDto far) {

    var navnFar = far.getNavn().sammensattNavn();

    var navnMor = mor.getNavn().sammensattNavn();

    assertAll(
        () -> assertThat(dokumenttekst).contains("Mother"),
        () -> assertThat(dokumenttekst).contains("Social security number: " + mor.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Date of birth: " + mor.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Name: " + navnMor),
        () -> assertThat(dokumenttekst).contains("Father"),
        () -> assertThat(dokumenttekst).contains("Social security number: " + far.getFoedselsnummer()),
        () -> assertThat(dokumenttekst).contains("Date of birth: " + far.getFoedselsdato().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))),
        () -> assertThat(dokumenttekst).contains("Name: " + navnFar)
    );
  }
}
