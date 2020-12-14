package no.nav.farskapsportal.consumer.pdl.stub;

import java.time.LocalDateTime;
import java.util.Map;
import lombok.Getter;
import lombok.Value;
import no.nav.farskapsportal.consumer.pdl.api.KjoennTypeDto;

@Value
@Getter
public class HentPersonKjoenn implements HentPersonSubQuery {

  String query;

  public HentPersonKjoenn(KjoennTypeDto kjoenn) {
    this.query = buildQueryKjoenn(kjoenn, "sagga-dagga", false);
  }

  public HentPersonKjoenn(Map<KjoennTypeDto, LocalDateTime> kjoennshistorikk) {
    this.query = buildQueryKjoennMedHistorikk(kjoennshistorikk);
  }

  private String buildQueryKjoennMedHistorikk(Map<KjoennTypeDto, LocalDateTime> input) {
    if (input == null || input.isEmpty()) {
      return String.join("\n", " \"kjoenn\": [", "]");
    } else {

      var startingElements = String.join("\n", " \"kjoenn\": [");
      var closingElements = String.join("\n", "]");

      var kjoennshistorikk = new StringBuilder();
      kjoennshistorikk.append(startingElements);

      var count = 0;
      for (Map.Entry<KjoennTypeDto, LocalDateTime> kjoenn : input.entrySet()) {
        var historisk = input.size() > 1 && (count == 0 || count < input.size() - 2);
        kjoennshistorikk.append(hentKjoennElement(kjoenn.getKey(), kjoenn.getValue(), "123", historisk));
        if (input.size() > 1 && (count == 0 || count > input.size() - 1)) {
          kjoennshistorikk.append(",");
        }
        count++;
      }

      kjoennshistorikk.append(closingElements);
      return kjoennshistorikk.toString();
    }
  }

  private String buildQueryKjoenn(KjoennTypeDto kjoenn, String opplysningsId, boolean historisk) {
    if (kjoenn == null) {
      return String.join("\n", " \"kjoenn\": [", "]");
    } else {
      var startingElements = String.join("\n", " \"kjoenn\": [");
      var closingElements = String.join("\n", "]");

      return startingElements + hentKjoennElement(kjoenn, null, opplysningsId, historisk) + closingElements;
    }
  }

  private String hentKjoennElement(
      KjoennTypeDto kjoenn, LocalDateTime gyldighetstidspunkt, String opplysningsId, boolean historisk) {

    var kjoennElement = new StringBuilder();

    kjoennElement.append(String.join("\n", " {", " \"kjoenn\": \"" + kjoenn + "\","));
    if (gyldighetstidspunkt != null) {
      kjoennElement.append(hentFolkerigstermetadataElement(gyldighetstidspunkt));
      kjoennElement.append(",");
    }
    kjoennElement.append(hentMetadataElement(opplysningsId, historisk));
    kjoennElement.append(String.join("\n", " }"));

    return kjoennElement.toString();
  }

  private String hentFolkerigstermetadataElement(LocalDateTime gyldighetstidspunkt) {
    return String.join(
        "\n",
        " \"folkeregistermetadata\": {",
        "   \"gyldighetstidspunkt\": \"" + gyldighetstidspunkt + "\"",
        " }");
  }

  private String hentMetadataElement(String opplysningsId, boolean historisk) {
    return String.join(
        "\n",
        " \"metadata\": {",
        "   \"historisk\": \"" + historisk + "\",",
        "   \"opplysningsId\": \"" + opplysningsId + "\",",
        "   \"master\": \"Freg\"",
        " }");
  }
}
