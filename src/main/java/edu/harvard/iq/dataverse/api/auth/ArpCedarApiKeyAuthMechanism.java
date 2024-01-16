package edu.harvard.iq.dataverse.api.auth;

import edu.harvard.iq.dataverse.arp.ArpCedarAuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.User;

import jakarta.ejb.EJB;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import java.util.logging.Logger;

/**
 * AuthMechanism to allow using apiKey header o key query param with a CEDAR provided token
 * as associated with a Dataverse user via AuthenticatedUserArp.
 *
 * Note: while this is not an annotated EJB, this will be injected as an EJB into CompoundAuthMechanism
 * and so we can use @EJB and @Context inside it.
 */
public class ArpCedarApiKeyAuthMechanism implements AuthMechanism
{
    private static final Logger logger = Logger.getLogger(ArpCedarApiKeyAuthMechanism.class.getCanonicalName());

    @EJB
    protected ArpCedarAuthenticationServiceBean cedarAuthSvc;

    @Context
    protected HttpServletRequest httpRequest;

    @Override
    public User findUserFromRequest(ContainerRequestContext crc) throws WrappedAuthErrorResponse
    {
        return cedarAuthSvc.lookupUser(getRequestApiKey(crc));
    }

    private String getRequestApiKey(ContainerRequestContext containerRequestContext) {
        String headerParamApiKey = containerRequestContext.getHeaderString(ApiKeyAuthMechanism.DATAVERSE_API_KEY_REQUEST_HEADER_NAME);
        String queryParamApiKey = containerRequestContext.getUriInfo().getQueryParameters().getFirst(ApiKeyAuthMechanism.DATAVERSE_API_KEY_REQUEST_PARAM_NAME);

        return headerParamApiKey != null ? headerParamApiKey : queryParamApiKey;
    }
}
