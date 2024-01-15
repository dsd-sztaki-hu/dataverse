package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;

import jakarta.ejb.Stateless;
import jakarta.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.util.List;

@Stateless
@Named
public class ArpCedarAuthenticationServiceBean
{
    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;

     public AuthenticatedUserArp findUserForCedarToken(String cedarToken) {
        Query query = em.createNamedQuery("AuthenticatedUserArp.findOneForCedarToken");
        query.setParameter("cedarToken", cedarToken);
        List<AuthenticatedUserArp> res = query.getResultList();
        if (res.isEmpty()) {
            return null;
        }
        return res.get(0);
    }

    public AuthenticatedUser lookupUser(String cedarToken)
    {
        AuthenticatedUserArp user = findUserForCedarToken(cedarToken);
        if (user == null) {
            return null;
        }
        return user.getUser();
    }

    public AuthenticatedUserArp findAuthenticatedUserArpById(Long id) {
         return em.find(AuthenticatedUserArp.class, id);
    }

    public AuthenticatedUserArp createNewAuthenticatedUserArp() {
         return em.merge(new AuthenticatedUserArp());
    }
}
