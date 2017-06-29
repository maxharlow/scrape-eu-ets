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
    return document('.bgtitlelist .resultlink:nth-of-type(2)').get().map((link, i) => {
        return {
            uri: 'http://ec.europa.eu' + Cheerio(link).attr('href'),
            code: response.request.code,
            phase: i + 1
        }
    })
}

function pages(response) {
    console.log('Scraping ' + response.request.code + ' phase ' + response.request.phase + ' pages...')
    const document = Cheerio.load(response.body)
    const pages = document('[name="resultList.lastPageNumber"]').attr('value')
    return Array.from({ length: Number(pages) }).map((_, i) => {
        const page = i + 1
        return {
            uri: response.request.href + '&resultList.currentPageNumber=' + page,
            code: response.request.code,
            phase: response.request.phase,
            page
        }
    })
}

function details(response) {
    console.log('Scraping ' + response.request.code + ' phase ' + response.request.phase + ' page ' + response.request.page + '...')
    const document = Cheerio.load(response.body)
    return document('.bgcelllist a:first-of-type').get().map(link => {
        return 'http://ec.europa.eu' + Cheerio(link).attr('href')
    })
}

function fields(response) {
    const document = Cheerio.load(response.body)
    return {
        url: response.request.href.replace(/;EUROPA_JSESSIONID=.+\?/, '?'),
        installationId: document('.bgcelllist span').eq(0).text().trim(),
        installationName: document('.bgcelllist span').eq(1).text().trim(),
        installationPermitId: document('.bgcelllist span').eq(2).text().trim(),
        installationPermitEntryDate: document('.bgcelllist span').eq(3).text().trim(),
        installationPermitExpiryDate: document('.bgcelllist span').eq(4).text().trim(),
        installationSubsidiaryCompany: document('.bgcelllist span').eq(5).text().trim(),
        installationParentCompany: document('.bgcelllist span').eq(6).text().trim(),
        installationEprtr: document('.bgcelllist span').eq(7).text().trim(),
        installationAddressLine1: document('.bgcelllist span').eq(8).text().trim(),
        installationAddressLine2: document('.bgcelllist span').eq(9).text().trim(),
        installationAddressPostcode: document('.bgcelllist span').eq(10).text().trim(),
        installationAddressCity: document('.bgcelllist span').eq(11).text().trim(),
        installationAddressCountry: document('.bgcelllist span').eq(12).text().trim(),
        installationAddressLatitude: document('.bgcelllist span').eq(13).text().trim(),
        installationAddressLongitude: document('.bgcelllist span').eq(14).text().trim(),
        installationActivity: document('.bgcelllist span').eq(15).text().trim(),
        installationContactName: document('.bgcelllist span').eq(16).text().trim(),
        installationContactAddress: document('.bgcelllist span').eq(17).text().trim()
            + (document('.bgcelllist span').eq(18).text().trim() ? ', ' + document('.bgcelllist span').eq(18).text().trim() : '')
            + (document('.bgcelllist span').eq(20).text().trim() ? ', ' + document('.bgcelllist span').eq(20).text().trim() : '')
            + (document('.bgcelllist span').eq(19).text().trim() ? ', ' + document('.bgcelllist span').eq(19).text().trim() : '')
            + (document('.bgcelllist span').eq(21).text().trim() ? ', ' + document('.bgcelllist span').eq(21).text().trim() : ''),
        accountAdministrator: document('.bgcelllist span').eq(22).text().trim(),
        accountType: document('.bgcelllist span').eq(23).text().trim(),
        accountHolderName: document('.bgcelllist span').eq(24).text().trim(),
        accountCompanyNumber: document('.bgcelllist span').eq(26).text().trim(),
        accountStatus: document('.bgcelllist span').eq(27).text().trim(),
        accountContactType: document('.bgcelllist span').eq(28).text().trim(),
        accountContactName: document('.bgcelllist span').eq(29).text().trim(),
        accountContactAddress: document('.bgcelllist span').eq(30).text().trim()
            + (document('.bgcelllist span').eq(31).text().trim() ? ', ' + document('.bgcelllist span').eq(31).text().trim() : '')
            + (document('.bgcelllist span').eq(33).text().trim() ? ', ' + document('.bgcelllist span').eq(33).text().trim() : '')
            + (document('.bgcelllist span').eq(32).text().trim() ? ', ' + document('.bgcelllist span').eq(32).text().trim() : '')
            + (document('.bgcelllist span').eq(34).text().trim() ? ', ' + document('.bgcelllist span').eq(34).text().trim() : '')
    }
}

Highland(['http://ec.europa.eu/environment/ets/napMgt.do'])
    .flatMap(http)
    .flatMap(countries)
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
