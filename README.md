Scrape EU ETS Allocations
=========================

The [European Union Emission Trading Scheme](https://ec.europa.eu/clima/policies/ets) is a system for managing greenhouse gas emissions in the European Union. There is a cap on the total amount of greenhouse gas emissions allowed, which is then converted into tradable emission allowances. Those allowances are allocated by *free allocations* and *auctions* to companies. One allowance gives the holder the right to emit the equvalent of one tonne of carbon dioxide. Participants must monitor and report their emissions each year to the EU, alongside the necessary number of emission allowances, or be fined. [Details of those allocations are published by the EU](http://ec.europa.eu/environment/ets/napMgt.do).

This scrapes all the available allocation records into a CSV file.

Requires [Node](https://nodejs.org/).

Install the dependencies with `npm install`, then run `node eu-ets-allocations`. Produces a file named `eu-ets-allocations.csv`.
