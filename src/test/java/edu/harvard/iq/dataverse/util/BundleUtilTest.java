package edu.harvard.iq.dataverse.util;

import java.util.Arrays;
import java.util.List;
import java.util.MissingResourceException;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BundleUtilTest {

    @Test
    public void testGetStringFromBundle() {
        assertEquals(null, BundleUtil.getStringFromBundle(null));
        assertEquals(null, BundleUtil.getStringFromBundle(""));
        assertEquals(null, BundleUtil.getStringFromBundle("junkKeyWeDoNotExpectToFind"));
        assertEquals("Search", BundleUtil.getStringFromBundle("search"));
        assertEquals("Error validating the username, email address, or password. Please try again. If the problem persists, contact an administrator.", BundleUtil.getStringFromBundle("login.error"));
    }

    @Test
    public void testGetStringFromBundleWithArguments() {
        assertEquals(null, BundleUtil.getStringFromBundle(null, (List<String>) null));
        String actual = BundleUtil.getStringFromBundle("dataverse.create.success", Arrays.asList("http://guides.dataverse.org/en", "4.0"));
        String expected = "You have successfully created your dataverse! To learn more about what you can do with your dataverse, check out the <a href=\"http://guides.dataverse.org/en/4.0/user/dataverse-management.html\" title=\"Dataverse Management - Dataverse User Guide\" target=\"_blank\">User Guide</a>.";
        assertEquals(expected, actual);
        assertEquals("Your new dataverse named "
                + "dvName (view at dvUrl ) "
                + "was created in parentDvName (view at parentDvUrl ). To learn more "
                + "about what you can do with your dataverse, check out "
                + "the Dataverse Management - User Guide at "
                + "http://guides.dataverse.org/en/4.0/user/dataverse-management.html .",
                BundleUtil.getStringFromBundle("notification.email.createDataverse",
                        Arrays.asList("dvName", "dvUrl", "parentDvName", "parentDvUrl", "http://guides.dataverse.org/en", "4.0")));
        assertEquals("Your new dataset named dsName (view at dsUrl ) "
                + "was created in parentDvName (view at parentDvUrl ). "
                + "To learn more about what you can do with a dataset, "
                + "check out the Dataset Management - User Guide at "
                + "http://guides.dataverse.org/en/4.0/user/dataset-management.html .",
                BundleUtil.getStringFromBundle("notification.email.createDataset",
                        Arrays.asList("dsName", "dsUrl", "parentDvName", "parentDvUrl", "http://guides.dataverse.org/en", "4.0")));
        assertEquals("There are no dataverses, datasets, or files that match your search. "
                + "Please try a new search by using other or broader terms. You can also check out "
                + "the <a href=\"http://guides.dataverse.org/en/4.2/user/find-use-data.html\" title=\"Finding &amp; Using "
                + "Data - Dataverse User Guide\" target=\"_blank\">search guide</a> for tips.",
                BundleUtil.getStringFromBundle("dataverse.results.empty.zero",
                        Arrays.asList("http://guides.dataverse.org/en", "4.2")));
        assertEquals("There are no search results based on how you have narrowed your search. You can check out "
                + "the <a href=\"http://guides.dataverse.org/en/4.2/user/find-use-data.html\" title=\"Finding &amp; Using "
                + "Data - Dataverse User Guide\" target=\"_blank\">search guide</a> for tips.",
                BundleUtil.getStringFromBundle("dataverse.results.empty.hidden",
                        Arrays.asList("http://guides.dataverse.org/en", "4.2")));
        assertEquals("The saved search has been successfully linked to "
                + "<a href=\"/dataverse/dvAlias\" title=\"DV Name\">DV Name</a>.",
                BundleUtil.getStringFromBundle("dataverse.saved.search.success",
                        Arrays.asList("<a href=\"/dataverse/dvAlias\" title=\"DV Name\">DV Name</a>")));
        assertEquals("Your institutional log in for TestShib Test IdP matches an email address already being used for a Dataverse "
                + "account. By entering your current Dataverse password below, your existing Dataverse account can be "
                + "converted to use your institutional log in. After converting, you will only need to use your institutional log in.",
                BundleUtil.getStringFromBundle("shib.welcomeExistingUserMessage",
                        Arrays.asList("TestShib Test IdP")));
        assertEquals("Your institutional log in for your institution matches an email address already being used for a Dataverse "
                + "account. By entering your current Dataverse password below, your existing Dataverse account can be "
                + "converted to use your institutional log in. After converting, you will only need to use your institutional log in.",
                BundleUtil.getStringFromBundle("shib.welcomeExistingUserMessage",
                        Arrays.asList(BundleUtil.getStringFromBundle("shib.welcomeExistingUserMessageDefaultInstitution"))));
    }

    @Test
    public void testGetStringFromBundleWithArgumentsAndSpecificBundle() {
        assertEquals(null, BundleUtil.getStringFromBundle(null, null, null));
        assertEquals("Search", BundleUtil.getStringFromBundle("search", null, BundleUtil.getResourceBundle("Bundle")));
    }
    
    @Test
    public void testStringFromPropertyFile() {
        assertEquals("Archive", BundleUtil.getStringFromPropertyFile("application/zip","MimeTypeFacets"));
    }

    //To assure that the MissingResourceException bubble up from this call
    @Test
    void testStringFromPropertyFileException() {
        assertThrows(MissingResourceException.class, () -> BundleUtil.getStringFromPropertyFile("FAKE","MimeTypeFacets"));
    }
    
    //To assure MissingResourceException is caught when calling normal bundle calls
    @Test
    void testNoErrorNonExistentStringBundle() {
        assertDoesNotThrow(() -> BundleUtil.getStringFromBundle("FAKE", null, BundleUtil.getResourceBundle("MimeTypeFacets")));
    }

    @Test
    void applyLocaleFromUsesLangParameter() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("lang")).thenReturn("hu");
        try {
            assertTrue(BundleUtil.applyLocaleFrom(request));
            assertEquals("hu", BundleUtil.getRequestLocale().getLanguage());
        } finally {
            BundleUtil.clearRequestLocale();
        }
    }

    @Test
    void applyLocaleFromIgnoresRequestOutsideServlet() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("lang")).thenThrow(new IllegalStateException(
                "WELD-000710: Cannot inject HttpServletRequest outside of a Servlet request"));
        try {
            assertNull(BundleUtil.localeFrom(request));
            assertFalse(BundleUtil.applyLocaleFrom(request));
            assertNull(BundleUtil.getRequestLocale());
        } finally {
            BundleUtil.clearRequestLocale();
        }
    }

    @Test
    void applyLocaleKeepsAnExistingLocale() {
        BundleUtil.setRequestLocale(BundleUtil.localeFromCode("hu"));
        try {
            assertFalse(BundleUtil.applyLocale(BundleUtil.localeFromCode("en")));
            assertEquals("hu", BundleUtil.getRequestLocale().getLanguage());
        } finally {
            BundleUtil.clearRequestLocale();
        }
    }
}
