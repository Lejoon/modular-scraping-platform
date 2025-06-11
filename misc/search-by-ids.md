API: /api/v2/united-publishers/search-by-ids

## Overview
This is a POST request to the AppMagic API endpoint /api/v2/united-publishers/search-by-ids. The purpose of this query is to retrieve detailed information about one or more "united publishers" by providing their specific store and publisher IDs. A "united publisher" is an entity created by AppMagic to group all of a single company's publisher accounts across different app stores (e.g., Google Play, Apple App Store) into one profile.

Request Body
The request body is a JSON object containing a list of publisher identifiers to search for.

ids: (Array of Objects) An array where each object specifies a single publisher to look up. You can include multiple objects to search for several publishers in one request.
store: (Integer) A numerical code for the app store. For example, 1 might represent the Google Play Store, while 2 or 3 could represent the Apple App Store.
store_publisher_id: (String) The unique identifier for the publisher as it appears in that specific store.
Example:

{
  "ids": [
    {
      "store": 1,
      "store_publisher_id": "8053728788464134315" 
    }
  ]
}

## Response Body
The response is a JSON object containing the data for the found publisher(s).

data: (Array of Objects) An array containing the full profile for each "united publisher" that matched the search criteria. Each object in this array represents one united publisher and contains the following keys:
id: (Integer) The unique internal AppMagic ID for the united publisher.
url: (String) A URL to the publisher's page on the AppMagic website (empty in this example).
apps: (Integer) The total count of all app listings associated with this publisher across all their store profiles.
united_apps: (Integer) The number of unique applications after de-duplicating apps that are on multiple stores.
min_release_date: (String) An ISO 8601 timestamp for the release date of the publisher's oldest app.
min_data_date: (String) An ISO 8601 timestamp for the earliest date that AppMagic has performance data for this publisher.
name: (String) The primary, unified name of the publisher (e.g., "G5 Entertainment").
alternative_names: (Array of Strings or Null) A list of other names the publisher is known by, or null if none.
score: (Integer) An internal AppMagic performance score for the publisher.
downloads: (Integer) An estimate of the publisher's total downloads.
revenue: (Integer) An estimate of the publisher's total revenue.
countries: (Array of Strings) A list of two-letter country codes (e.g., "US", "GB", "JP") where the publisher's apps are available.
headquarter: (String) The two-letter country code for the publisher's headquarters location.
linkedin_headcount: (Integer) The number of employees listed on the publisher's LinkedIn page.
dataCountries: (Array of Strings) A list of two-letter country codes for which AppMagic has detailed performance data (downloads/revenue) for this publisher.
store_ids: (Array of Strings) A list of composite IDs that combine the store number and the store-specific publisher ID (e.g., 1_8053728788464134315).
publisherIds: (Array of Objects) A detailed breakdown of the individual publisher accounts that make up this united publisher. Each object contains:
store: (Integer) The store code.
store_publisher_id: (String) The ID in that store.
name: (String) The publisher's name as it appears in that store.
first_adv_imp: (String) The date (YYYY-MM-DD) of the first observed ad impression for this publisher.
first_app_ad: (String) The date (YYYY-MM-DD) when an ad was first observed inside one of the publisher's apps.
publisherType: (Object) An object describing the entity type. It has a name key, which is "united_publisher".
top_applications_info: (Array of Objects) A list of the publisher's top-performing applications. Each object includes:
united_application_id: (Integer) The unique AppMagic ID for the app.
store: (Integer) The store the app is on.
store_application_id: (String) The app's ID in that store (e.g., "com.g5e.sherlock.android").
name: (String) The name of the application.
icon_url: (String) A URL for the application's icon.
total_score: (Integer) An AppMagic performance score for the app.
mongo_id: (String) The unique document ID from the backend database (likely MongoDB), used for internal reference.
