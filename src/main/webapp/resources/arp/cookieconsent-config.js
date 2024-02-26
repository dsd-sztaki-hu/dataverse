//created at: https://playground.cookieconsent.orestbida.com/

// This is strange, but we actually need the .xhtml at the end because it is imported from an XHTML file.
import './cookieconsent.umd.js.xhtml';

CookieConsent.run({
    guiOptions: {
        consentModal: {
            layout: "box",
            position: "middle center",
            equalWeightButtons: false,
            flipButtons: false
        },
        preferencesModal: {
            layout: "box",
            position: "right",
            equalWeightButtons: false,
            flipButtons: false
        }
    },
    categories: {
        necessary: {
            readOnly: true
        },
        analytics: {}
    },
    language: {
        default: localeCode, // comes from dataverse_template.xhtml
        translations: {
            hu: {
                consentModal: {
                    title: "Süti beállítások",
                    description: "A weboldalunkon a megfelelő felhasználói élmény biztosítása érdekében sütiket (cookie-kat) használunk.",
                    acceptAllBtn: "Összes elfogadása",
                    acceptNecessaryBtn: "Összes visszautasítása",
                    showPreferencesBtn: "Testreszabás",
                    footer: "<a href=\"https://researchdata.hu/adatkezelesi-tajekoztato\" target='_blank'>Adatkezelési tájékoztató</a>\n<a href=\"https://researchdata.hu/altalanos-felhasznalasi-feltetelek\" target='_blank'>Általános felhasználási feltételek</a>"
                },
                preferencesModal: {
                    title: "Beállítások testreszabása",
                    acceptAllBtn: "Összes elfogadása",
                    acceptNecessaryBtn: "Összes visszautasítása",
                    savePreferencesBtn: "Beállítások mentése",
                    closeIconLabel: "Bezárás",
                    serviceCounterLabel: "szolgáltatás",
                    sections: [
                        {
                            title: "Sütik használata",
                            description: "A sütik kis fájlok, amelyeket a weboldalak helyeznek el a felhasználók eszközein adatgyűjtés céljából és a felhasználói élmény fokozására. Megjegyezhetik a beállításokat, és nyomon követhetik a böngészési szokásokat. Bármikor módosíthatja a sütikkel és az online követéssel kapcsolatos preferenciáit."
                        },
                        {
                            title: "Szolgáltatás használatához elengedhetetlen sütik <span class=\"pm__badge\">Mindig</span>",
                            description: "Ezek a sütik elengedhetetlenek a weboldal működéséhez, és nem kapcsolhatók ki.",
                            linkedCategory: "necessary"
                        },
                        {
                            title: "Analitikai sütik",
                            description: "Az analitikai és mérőeszközök segítenek nekünk megérteni, hogy a felhasználóink hogyan haszálják a sozlgáltatást és hogyan tudunk a felhaszálói élményen javítani.",
                            linkedCategory: "analytics"
                        },
                        {
                            title: "További információk",
                            description: "További információkért érdeklődjön <a class=\"cc__link\" href=\"https://researchdata.hu\" target='_blank'>researchdata.hu</a> oldalon!"
                        }
                    ]
                }
            },
            en: {
                consentModal: {
                    title: "Cookie settings",
                    description: "Our website uses cookies to improve your browsing experience. You can read more about our Privacy policy and Terms and condition on the links below.",
                    acceptAllBtn: "Accept all",
                    acceptNecessaryBtn: "Reject all",
                    showPreferencesBtn: "Manage preferences",
                    footer: "<a href=\"https://researchdata.hu/adatkezelesi-tajekoztato\" target='_blank'>Privacy Policy</a>\n<a href=\"https://researchdata.hu/en/general-terms-use\" target='_blank'>Terms and conditions</a>"
                },
                preferencesModal: {
                    title: "Cookie Preferences Center",
                    acceptAllBtn: "Accept all",
                    acceptNecessaryBtn: "Reject all",
                    savePreferencesBtn: "Save preferences",
                    closeIconLabel: "Close modal",
                    serviceCounterLabel: "Service|Services",
                    sections: [
                        {
                            title: "Cookie Usage",
                            description: "Cookies are small files that websites place on users' devices to collect data and enhance user experience. They can remember preferences, and track browsing habits. You can modify your preferences regarding cookies and tracking at any time."
                        },
                        {
                            title: "Strictly Necessary Cookies <span class=\"pm__badge\">Always Enabled</span>",
                            description: "These cookies are essential for the operation of the website and cannot be turned off.",
                            linkedCategory: "necessary"
                        },
                        {
                            title: "Analytics Cookies",
                            description: "Analytics help us understand how users interact with our services and how we can improve their experiences.",
                            linkedCategory: "analytics"
                        },
                        {
                            title: "More information",
                            description: "For any query in relation to policy on cookies and your choices, you can contact as <a class=\"cc__link\" href=\"#https://researchdata.hu/en\" target='_blank'>here</a>."
                        }
                    ]
                }
            }
        }
    },
    disablePageInteraction: true
});