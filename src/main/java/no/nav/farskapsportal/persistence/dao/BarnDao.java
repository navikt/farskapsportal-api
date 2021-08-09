package no.nav.farskapsportal.persistence.dao;

import java.util.Optional;
import no.nav.farskapsportal.persistence.entity.Barn;
import no.nav.farskapsportal.persistence.entity.Forelder;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BarnDao extends CrudRepository<Barn, Integer> {

  @Query("select b from Barn  b where b.foedselsnummer = :fnrBarn")
  Optional<Barn> henteBarnMedFnr(String fnrBarn);
}
