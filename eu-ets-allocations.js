const Highland = require('highland')
const Request = require('request')
const RetryMe = require('retry-me')
const Cheerio = require('cheerio')
const FS = require('fs')
const CSVWriter = require('csv-write-stream')

const http = Highland.wrapCallback((location, callback) => {
    const headers = { 'User-Agent': '' }
    const input = output => {
        Request.defaults({ headers })(location, (error, response) => {
            const failure = error ? error : (response.statusCode >= 400) ? new Error(response.statusCode) : null
            output(failure, response)
        })
    }
    RetryMe(input, { factor: 1.5 }, callback)
})

function countries(response) {
    console.log('Scraping countries...')
    const document = Cheerio.load(response.body)
    return document('.formComboListMedium .formOptionListMedium').get().map(country => {
        const code = Cheerio(country).attr('value')
        const uri = 'http://ec.europa.eu/environment/ets/nap.do'
            + '?search=Search'
            + '&nap.registryCodeArray=' + code
        return { uri, code }
    })
}

function results(response) {
    console.log('Scraping ' + response.request.code + ' results...')
    const document = Cheerio.load(response.body)
    const phases = document('#tblNapSearchResult tr').filter(':has(.bgcelllist)')
    return phases.get().map(row => {
        const phase = Cheerio.load(row)
        return {
            uri: 'http://ec.europa.eu' + phase('td:nth-of-type(5) a:nth-of-type(2)').attr('href'),
            country: phase('td:nth-of-type(1) > span').text().trim(),
            code: response.request.code,
            phase: phase('td:nth-of-type(2) > span').text().trim()
        }
    })
}

function pages(response) {
    console.log('Scraping ' + response.request.code + ' ' + response.request.phase + ' pages...')
    const document = Cheerio.load(response.body)
    const pages = document('[name="resultList.lastPageNumber"]').attr('value')
    return Array.from({ length: Number(pages) }).map((_, i) => {
        const page = i + 1
        return {
            uri: response.request.href + '&resultList.currentPageNumber=' + page,
            country: response.request.country,
            code: response.request.code,
            phase: response.request.phase,
            page
        }
    })
}

function details(response) {
    console.log('Scraping ' + response.request.code + ' ' + response.request.phase + ' page ' + response.request.page + '...')
    const document = Cheerio.load(response.body)
    return document('.bgcelllist a:first-of-type').get().map(link => {
        return {
            uri: 'http://ec.europa.eu' + Cheerio(link).attr('href'),
            country: response.request.country,
            code: response.request.code,
            phase: response.request.phase
        }
    })
}

function fields(response) {
    const document = Cheerio.load(response.body)
    return {
        url: response.request.href.replace(/;EUROPA_JSESSIONID=.+\?/, '?'),
        country: response.request.country,
        phase: response.request.phase,
        installationId: document('.bgcelllist span').eq(0).text().trim(),
        installationName: document('.bgcelllist span').eq(1).text().trim(),
        installationPermitId: document('.bgcelllist span').eq(2).text().trim(),
        installationPermitEntryDate: document('.bgcelllist span').eq(3).text().trim(),
        installationPermitExpiryDate: document('.bgcelllist span').eq(4).text().trim(),
        installationSubsidiaryCompany: document('.bgcelllist span').eq(5).text().trim(),
        installationParentCompany: document('.bgcelllist span').eq(6).text().trim(),
        installationEprtr: document('.bgcelllist span').eq(7).text().trim(),
        installationAddress: (document('.bgcelllist span').eq(8).text().trim().replace('XXX') ? document('.bgcelllist span').eq(8).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq( 9).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq( 9).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(11).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(11).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(10).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(10).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(12).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(12).text().trim().replace('XXX', '') : ''),
        installationAddressLatitude: document('.bgcelllist span').eq(13).text().trim(),
        installationAddressLongitude: document('.bgcelllist span').eq(14).text().trim(),
        installationActivity: document('.bgcelllist span').eq(15).text().trim(),
        installationContactName: document('.bgcelllist span').eq(16).text().trim(),
        installationContactAddress: (document('.bgcelllist span').eq(17).text().trim().replace('XXX', '') ? document('.bgcelllist span').eq(17).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(18).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(18).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(20).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(20).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(19).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(19).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(21).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(21).text().trim().replace('XXX', '') : ''),
        accountAdministrator: document('.bgcelllist span').eq(22).text().trim(),
        accountType: document('.bgcelllist span').eq(23).text().trim(),
        accountHolderName: document('.bgcelllist span').eq(24).text().trim(),
        accountCompanyNumber: document('.bgcelllist span').eq(26).text().trim(),
        accountStatus: document('.bgcelllist span').eq(27).text().trim(),
        accountContactType: document('.bgcelllist span').eq(28).text().trim(),
        accountContactName: document('.bgcelllist span').eq(29).text().trim(),
        accountContactAddress: (document('.bgcelllist span').eq(30).text().trim().replace('XXX', '') ? document('.bgcelllist span').eq(30).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(31).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(31).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(33).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(33).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(32).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(32).text().trim().replace('XXX', '') : '')
            + (document('.bgcelllist span').eq(34).text().trim().replace('XXX', '') ? ', ' + document('.bgcelllist span').eq(34).text().trim().replace('XXX', '') : '')
    }
}

Highland(['http://ec.europa.eu/environment/ets/napMgt.do'])
    .flatMap(http)
    .flatMap(countries)
    .filter(country => country.code === 'GB')
    .flatMap(http)
    .flatMap(results)
    .flatMap(http)
    .flatMap(pages)
    .flatMap(http)
    .flatMap(details)
    .flatMap(http)
    .map(fields)
    .errors(e => console.error(e.stack))
    .through(CSVWriter())
    .pipe(FS.createWriteStream('eu-ets-allocations.csv'))
