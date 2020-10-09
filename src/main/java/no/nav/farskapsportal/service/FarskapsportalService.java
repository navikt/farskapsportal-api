package no.nav.farskapsportal.service;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import no.nav.bidrag.commons.web.HttpResponse;
import no.nav.farskapsportal.api.Kjoenn;
import no.nav.farskapsportal.api.KontrollerePersonopplysningerRequest;
import no.nav.farskapsportal.consumer.pdl.PdlApiConsumer;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.exception.FeilKjoennPaaOppgittFarException;
import no.nav.farskapsportal.exception.OppgittNavnStemmerIkkeMedRegistrertNavnException;
import no.nav.farskapsportal.exception.PersonIkkeFunnetException;
import org.apache.commons.lang3.Validate;
import org.springframework.http.HttpStatus;

@Builder
@Slf4j
public class FarskapsportalService {

  public static final String FEIL_NAVN =
      "Oppgitt navn til person stemmer ikke med navn slik det er registreret i Folkeregisteret";
  private final PdlApiConsumer pdlApiConsumer;

  public HttpResponse<Kjoenn> henteKjoenn(String foedselsnummer) {
    return pdlApiConsumer.henteKjoenn(foedselsnummer);
  }

  public HttpResponse<?> riktigNavnOppgittForFar(KontrollerePersonopplysningerRequest request) {
    Validate.isTrue(request.getFoedselsnummer() != null);

    var responsMedNavn = pdlApiConsumer.hentNavnTilPerson(request.getFoedselsnummer());
    var navnDto = responsMedNavn.getResponseEntity().getBody();

    if (navnDto == null){
      throw new PersonIkkeFunnetException("Responsen fra PDL mangler informasjon om person");
    }

    // Validere input
    Validate.isTrue(request.getFornavn() != null);
    Validate.isTrue(request.getEtternavn() != null);
    if (navnDto.getMellomnavn() != null) {
      Validate.isTrue(navnDto.getMellomnavn().equalsIgnoreCase(request.getMellomnavn()));
    } else {
      Validate.isTrue(request.getMellomnavn() == null);
    }

    navnekontroll(request, navnDto);

    var kjoennFar = henteKjoenn(request.getFoedselsnummer()).getResponseEntity().getBody();
    if (!Kjoenn.MANN.equals(kjoennFar)) {
      throw new FeilKjoennPaaOppgittFarException("Oppgitt far er ikke mann!");
    }

    log.info("Sjekk av oppgitt fars fødselsnummer, navn, og kjønn er gjennomført uten feil");

    return HttpResponse.from(HttpStatus.OK);
  }

  private void navnekontroll(
      KontrollerePersonopplysningerRequest navnOppgitt, NavnDto navnFraRegister) {
    boolean fornavnStemmer = navnFraRegister.getFornavn().equalsIgnoreCase(navnOppgitt.getFornavn());
    boolean mellomnavnStemmer =
        navnFraRegister.getMellomnavn() == null
            ? navnOppgitt.getMellomnavn() == null
            : navnOppgitt.getMellomnavn().equalsIgnoreCase(navnOppgitt.getMellomnavn());
    boolean etternavnStemmer = navnFraRegister.getEtternavn().equalsIgnoreCase(navnOppgitt.getEtternavn());

    if (!fornavnStemmer || !mellomnavnStemmer || !etternavnStemmer) {
      Map<String, Boolean> navnesjekk = new HashMap<>();
      navnesjekk.put("fornavn", fornavnStemmer);
      navnesjekk.put("mellomnavn", mellomnavnStemmer);
      navnesjekk.put("etternavn", etternavnStemmer);

      StringBuffer sb = new StringBuffer();
      navnesjekk.forEach((k, v) -> leggeTil(!fornavnStemmer, k, sb));

      log.error("Navnekontroll feilet. Status navnesjekk (false = feilet): {}", navnesjekk);

      throw new OppgittNavnStemmerIkkeMedRegistrertNavnException(
          "Oppgitt navn til person stemmer ikke med navn slik det er registreret i Folkeregisteret");
    }

    log.info("Navnekontroll gjennomført uten feil");
  }

  private void leggeTil(boolean skalLeggesTil, String navnedel, StringBuffer sb) {
    if (skalLeggesTil) {
      sb.append(navnedel);
    }
  }
}
