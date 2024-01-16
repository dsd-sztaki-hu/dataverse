package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;

import jakarta.persistence.*;
import java.io.Serializable;

/**
 * Additional ARP related values associated with an AuthenticatedUser.
 *
 * For now, we just add the CEDAR api key/token to the AuthenticatedUser so that a user may be authenticated
 * using either it DV or CEDAR key.
 */
@NamedQueries({
        // This should give a single result
        @NamedQuery(name = "AuthenticatedUserArp.findOneForCedarToken",
                query = "SELECT o FROM AuthenticatedUserArp o WHERE o.cedarToken=:cedarToken ORDER BY o.id"),
})
@Entity
@Table(indexes = {@Index(columnList="cedar_token")})
public class AuthenticatedUserArp implements Serializable
{
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne
    private AuthenticatedUser user;

    private String cedarToken;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    public void setUser(AuthenticatedUser user)
    {
        this.user = user;
    }

    public String getCedarToken()
    {
        return cedarToken;
    }

    public void setCedarToken(String cedarToken)
    {
        this.cedarToken = cedarToken;
    }
}
