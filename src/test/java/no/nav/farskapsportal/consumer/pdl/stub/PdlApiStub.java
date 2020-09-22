package no.nav.farskapsportal.consumer.pdl.stub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

@Slf4j
@Component
public class PdlApiStub {

    @Value("${urls.pdl-api.graphql-endpoint-path}")
    private String pdlApiGraphqlEndpoint;

    private static String stubHentPerson(List<HentPersonSubQuery> subQueries) {

        var startingElements = String.join("\n",
                " {",
                " \"data\": {",
                " \"hentPerson\": {"
        );

        var closingElements = String.join("\n",
                "}",
                "}",
                "}"
        );

        var stubResponse = new StringBuffer();
        stubResponse.append(startingElements);

        var count = 0;
        for(HentPersonSubQuery subQuery:subQueries) {
            stubResponse.append(subQuery.getQuery());
            if (subQueries.size() > 1 && (count == 0 || count > subQueries.size()-1)) {
                stubResponse.append(",");
            }
            count++;
        }

        stubResponse.append(closingElements);

        return stubResponse.toString();
    }

    public void runPdlApiHentPersonStub(List<HentPersonSubQuery> subQueries) {
        stubFor(post(urlEqualTo(pdlApiGraphqlEndpoint))
                .willReturn(
                        aResponse()
                                .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                                .withStatus(200)
                                .withBody(stubHentPerson(subQueries)))
        );
    }
}

