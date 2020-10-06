package no.nav.farskapsportal.consumer.pdl.stub;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.contract.spec.internal.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PdlApiStub {

  @Value("${urls.pdl-api.graphql-endpoint}")
  private String pdlApiGraphqlEndpoint;

  private static String stubHentPerson(List<HentPersonSubQuery> subQueries) {

    var startingElements = String.join("\n", " {", " \"data\": {", " \"hentPerson\": {");

    var closingElements = String.join("\n", "}", "}", "}");

    var stubResponse = new StringBuilder();
    stubResponse.append(startingElements);

    var count = 0;
    for (HentPersonSubQuery subQuery : subQueries) {
      stubResponse.append(subQuery.getQuery());
      if (subQueries.size() > 1 && (count == 0 || count > subQueries.size() - 1)) {
        stubResponse.append(",");
      }
      count++;
    }

    stubResponse.append(closingElements);

    return stubResponse.toString();
  }

  public void runPdlApiHentPersonStub(List<HentPersonSubQuery> subQueries) {
    stubFor(
        post(urlEqualTo(pdlApiGraphqlEndpoint))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                    .withStatus(HttpStatus.OK)
                    .withBody(stubHentPerson(subQueries))));
  }

  public void runPdlApiHentPersonFantIkkePersonenStub() {
    stubFor(
        post(urlEqualTo(pdlApiGraphqlEndpoint))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                    .withStatus(HttpStatus.OK)
                    .withBody(
                        String.join(
                            "\n",
                            " {",
                            "\"errors\": [",
                            "{",
                            "\"message\": \"Fant ikke person\",",
                            "\"locations\": [",
                            "{",
                            "\"line\": 8,",
                            "\"column\": 3",
                            "}",
                            "],",
                            "\"path\": [",
                            "\"hentPerson\"",
                            "],",
                            "\"extensions\": {",
                            "\"code\": \"not_found\",",
                            "\"classification\": \"ExecutionAborted\"",
                            "}",
                            "}",
                            "],",
                            "\"data\": {",
                            "\"hentPerson\": null",
                            "}",
                            "}"))));
  }
}
