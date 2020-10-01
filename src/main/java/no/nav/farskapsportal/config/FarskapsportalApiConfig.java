package no.nav.farskapsportal.config;

import static no.nav.farskapsportal.FarskapsportalApiApplication.ISSUER;
import static no.nav.farskapsportal.consumer.sts.SecurityTokenServiceEndpointName.HENTE_IDTOKEN_FOR_SERVICEUSER;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import java.text.ParseException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import no.nav.bidrag.commons.ExceptionLogger;
import no.nav.bidrag.commons.web.CorrelationIdFilter;
import no.nav.farskapsportal.consumer.ConsumerEndpoint;
import no.nav.farskapsportal.consumer.pdl.PdlApiConsumer;
import no.nav.farskapsportal.consumer.pdl.PdlApiConsumerEndpointName;
import no.nav.farskapsportal.consumer.pdl.PdlApiHelsesjekkConsumer;
import no.nav.farskapsportal.consumer.sts.SecurityTokenServiceConsumer;
import no.nav.farskapsportal.service.FarskapsportalService;
import no.nav.security.token.support.core.context.TokenValidationContextHolder;
import no.nav.security.token.support.core.jwt.JwtToken;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RootUriTemplateHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
public class FarskapsportalApiConfig {

  public static final String X_API_KEY = "x-nav-apiKey";

  public static String hentPaaloggetPerson(String idToken) {
    log.info("Skal finne pålogget person fra id-token ");

    try {
      return hentPaaloggetPerson(parseIdToken(idToken));
    } catch (Exception e) {
      log.error("Klarte ikke parse " + idToken, e);

      if (e instanceof RuntimeException) {
        throw ((RuntimeException) e);
      }
      throw new IllegalArgumentException("Klarte ikke å parse " + idToken, e);
    }
  }

  private static String hentPaaloggetPerson(SignedJWT signedJWT) {
    try {
      return signedJWT.getJWTClaimsSet().getSubject();
    } catch (ParseException e) {
      throw new IllegalStateException("Kunne ikke hente pålogget person fra id-token", e);
    }
  }

  public static SignedJWT parseIdToken(String idToken) throws ParseException {
    return (SignedJWT) JWTParser.parse(idToken);
  }

  @Bean
  public ConsumerEndpoint consumerEndpoint() {
    return new ConsumerEndpoint();
  }

  @Bean
  SecurityTokenServiceConsumer securityTokenServiceConsumer(
      @Qualifier("sts") RestTemplate restTemplate,
      @Value("${urls.sts.base-url}") String baseUrl,
      @Value("${urls.sts.security-token-service-endpoint}") String endpoint,
      ConsumerEndpoint consumerEndpoint) {
    log.info("Oppretter SecurityTokenServiceConsumer med url {}", baseUrl);
    consumerEndpoint.addEndpoint(HENTE_IDTOKEN_FOR_SERVICEUSER, endpoint);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
    return new SecurityTokenServiceConsumer(restTemplate, consumerEndpoint);
  }

  @Bean
  public PdlApiConsumer pdlApiConsumer(
      @Qualifier("pdl-api") RestTemplate restTemplate,
      @Value("${urls.pdl-api.base-url}") String baseUrl,
      @Value("${urls.pdl-api.graphql-endpoint}") String pdlApiEndpoint,
      ConsumerEndpoint consumerEndpoint) {
    consumerEndpoint.addEndpoint(PdlApiConsumerEndpointName.PDL_API_GRAPHQL, pdlApiEndpoint);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
    log.info("Oppretter PdlApiConsumer med url {}", baseUrl);
    return PdlApiConsumer.builder()
        .restTemplate(restTemplate)
        .consumerEndpoint(consumerEndpoint)
        .build();
  }

  @Bean
  public PdlApiHelsesjekkConsumer pdlApiHelsesjekkConsumer(
      @Qualifier("pdl-api") RestTemplate restTemplate,
      @Value("${urls.pdl-api.base-url}") String baseUrl,
      @Value("${urls.pdl-api.graphql-endpoint}") String pdlApiEndpoint,
      ConsumerEndpoint consumerEndpoint) {
    consumerEndpoint.addEndpoint(PdlApiConsumerEndpointName.PDL_API_GRAPHQL, pdlApiEndpoint);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
    log.info("Oppretter PdlApiHelsesjekkConsumer med url {}", baseUrl);
    return PdlApiHelsesjekkConsumer.builder()
        .restTemplate(restTemplate)
        .consumerEndpoint(consumerEndpoint)
        .build();
  }

  @Bean
  public FarskapsportalService farskapsportalService(PdlApiConsumer pdlApiConsumer) {
    return FarskapsportalService.builder().pdlApiConsumer(pdlApiConsumer).build();
  }

  @Bean
  public ExceptionLogger exceptionLogger() {
    return new ExceptionLogger(FarskapsportalApiConfig.class.getSimpleName());
  }

  @Bean
  public CorrelationIdFilter correlationIdFilter() {
    return new CorrelationIdFilter();
  }

  @Bean
  public OidcTokenManager oidcTokenManager(
      TokenValidationContextHolder tokenValidationContextHolder) {
    return () ->
        Optional.ofNullable(tokenValidationContextHolder)
            .map(TokenValidationContextHolder::getTokenValidationContext)
            .map(tokenValidationContext -> tokenValidationContext.getJwtTokenAsOptional(ISSUER))
            .map(Optional::get)
            .map(JwtToken::getTokenAsString)
            .orElseThrow(() -> new IllegalStateException("Kunne ikke videresende Bearer token"));
  }

  @Bean
  public OidcTokenSubjectExtractor oidcTokenSubjectExtractor(OidcTokenManager oidcTokenManager) {
    return () -> hentPaaloggetPerson(oidcTokenManager.hentIdToken());
  }

  @FunctionalInterface
  public interface OidcTokenManager {
    String hentIdToken();
  }

  @FunctionalInterface
  public interface OidcTokenSubjectExtractor {
    String hentPaaloggetPerson();
  }
}
