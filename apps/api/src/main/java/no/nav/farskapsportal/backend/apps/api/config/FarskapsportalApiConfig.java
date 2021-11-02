package no.nav.farskapsportal.backend.apps.api.config;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_LIVE;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.util.HashMap;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import no.nav.bidrag.commons.ExceptionLogger;
import no.nav.bidrag.commons.web.CorrelationIdFilter;
import no.nav.bidrag.tilgangskontroll.SecurityUtils;
import no.nav.farskapsportal.backend.apps.api.FarskapsportalApiApplication;
import no.nav.farskapsportal.backend.apps.api.api.Skriftspraak;
import no.nav.farskapsportal.backend.apps.api.config.egenskaper.FarskapsportalApiEgenskaper;
import no.nav.farskapsportal.backend.apps.api.consumer.esignering.DifiESignaturConsumer;
import no.nav.farskapsportal.backend.apps.api.consumer.pdf.PdfGeneratorConsumer;
import no.nav.farskapsportal.backend.apps.api.service.FarskapsportalService;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
import no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon.BrukernotifikasjonConsumer;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import no.nav.farskapsportal.backend.libs.felles.service.PersonopplysningService;
import no.nav.farskapsportal.backend.libs.felles.util.Mapper;
import no.nav.security.token.support.core.context.TokenValidationContextHolder;
import no.nav.security.token.support.core.jwt.JwtToken;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Slf4j
@Configuration
@OpenAPIDefinition(
    info = @Info(title = "farskapsportal-api", version = "v1"),
    security = @SecurityRequirement(name = "bearer-key")
)
@ComponentScan("no.nav.farskapsportal.backend")
public class FarskapsportalApiConfig {

  @Bean
  public OpenAPI openAPI() {
    return new OpenAPI()
        .components(new Components()
            .addSecuritySchemes("bearer-key", new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT"))
        ).info(new io.swagger.v3.oas.models.info.Info().title("farskapsportal-api").version("v1"));
  }

  @Bean
  public FarskapsportalService farskapsportalService(BrukernotifikasjonConsumer brukernotifikasjonConsumer,
      FarskapsportalApiEgenskaper farskapsportalApiEgenskaper,
      DifiESignaturConsumer difiESignaturConsumer,
      PdfGeneratorConsumer pdfGeneratorConsumer,
      PersistenceService persistenceService,
      PersonopplysningService personopplysningService,
      Mapper mapper) {

    return FarskapsportalService.builder()
        .brukernotifikasjonConsumer(brukernotifikasjonConsumer)
        .farskapsportalApiEgenskaper(farskapsportalApiEgenskaper)
        .difiESignaturConsumer(difiESignaturConsumer)
        .pdfGeneratorConsumer(pdfGeneratorConsumer)
        .persistenceService(persistenceService)
        .personopplysningService(personopplysningService)
        .mapper(mapper).build();
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
  public OidcTokenManager oidcTokenManager(TokenValidationContextHolder tokenValidationContextHolder) {
    return () -> Optional.ofNullable(tokenValidationContextHolder).map(TokenValidationContextHolder::getTokenValidationContext)
        .map(tokenValidationContext -> tokenValidationContext.getJwtTokenAsOptional(FarskapsportalApiApplication.ISSUER)).map(Optional::get)
        .map(JwtToken::getTokenAsString)
        .orElseThrow(() -> new IllegalStateException("Kunne ikke videresende Bearer token"));
  }

  @Bean
  public OidcTokenSubjectExtractor oidcTokenSubjectExtractor(OidcTokenManager oidcTokenManager) {
    return () -> SecurityUtils.henteSubject(oidcTokenManager.hentIdToken());
  }

  @FunctionalInterface
  public interface OidcTokenManager {

    String hentIdToken();
  }

  @FunctionalInterface
  public interface OidcTokenSubjectExtractor {

    String hentPaaloggetPerson();
  }

  @Configuration
  @Profile(PROFILE_LIVE)
  public static class FlywayConfiguration {

    @Autowired
    public FlywayConfiguration(@Qualifier("dataSource") DataSource dataSource, @Value("${spring.flyway.placeholders.user}") String dbUserAsynkron)
        throws InterruptedException {
      Thread.sleep(30000);
      var placeholders = new HashMap<String, String>();
      placeholders.put("user_asynkron", dbUserAsynkron);

      Flyway.configure().dataSource(dataSource).placeholders(placeholders).load().migrate();
    }
  }

  public static class StringToEnumConverter implements Converter<String, Skriftspraak> {

    @Override
    public Skriftspraak convert(String source) {
      return Skriftspraak.valueOf(source.toUpperCase());
    }
  }

  @Configuration
  public static class WebConfig implements WebMvcConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
      registry.addConverter(new StringToEnumConverter());
    }
  }
}
