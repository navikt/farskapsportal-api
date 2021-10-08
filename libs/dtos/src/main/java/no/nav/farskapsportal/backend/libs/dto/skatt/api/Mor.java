package no.nav.farskapsportal.backend.libs.dto.skatt.api;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class Mor {

  @XmlElement
  private Foedselsnummer foedselsEllerDNummer;

  @XmlElement
  private Boolsk harSignert;


}
