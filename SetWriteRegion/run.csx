#r "Newtonsoft.Json"
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

static HttpClient httpClient = new HttpClient();

public async static Task Run(TimerInfo myTimer, ILogger log)
{
    var endpoint = System.Environment.GetEnvironmentVariable("MSI_ENDPOINT", EnvironmentVariableTarget.Process);
    var secret = System.Environment.GetEnvironmentVariable("MSI_SECRET", EnvironmentVariableTarget.Process);
    var cosmosDbResourceId = System.Environment.GetEnvironmentVariable("COSMOS_DB_RESOURCE_ID", EnvironmentVariableTarget.Process);

    // get token from MSI
    var tokenRequest = new HttpRequestMessage()
    {
        RequestUri = new Uri(endpoint + "?resource=https://management.azure.com/&api-version=2017-09-01"),
        Method = HttpMethod.Get
    };
    tokenRequest.Headers.Add("secret", secret);

    var tokenResp = await httpClient.SendAsync(tokenRequest);
    tokenResp.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
    dynamic result = await tokenResp.Content.ReadAsAsync<object>();

    string token = result.access_token;

    // get current failover policies
    var getDbAccountRequest = new HttpRequestMessage
    {
        RequestUri = new Uri($"https://management.azure.com{cosmosDbResourceId}?api-version=2015-04-08"),
        Method = HttpMethod.Get
    };
    getDbAccountRequest.Headers.Add("Authorization", "Bearer " + token);

    var dbAccountResp = await httpClient.SendAsync(getDbAccountRequest);
    dynamic dbAccountResult = await dbAccountResp.Content.ReadAsAsync<object>();
    IEnumerable<dynamic> locations = dbAccountResult.properties.readLocations;
    var locationNames = locations
        .OrderBy(l => (int)l.failoverPriority)
        .Select(l => (string)l.locationName);

    log.LogInformation("Current locations: " + string.Join(",", locationNames));

    // update write location
    var reversedLocationNames = locationNames.Reverse();

    var requestBody = new
    {
        failoverPolicies = reversedLocationNames
            .Select((n, i) => new
            {
                locationName = n,
                failoverPriority = i
            })
    };

    var requestBodyJson = JsonConvert.SerializeObject(requestBody);
    log.LogInformation(requestBodyJson);

    var changeWriteLocationRequest = new HttpRequestMessage
    {
        RequestUri = new Uri($"https://management.azure.com{cosmosDbResourceId}/failoverPriorityChange?api-version=2015-04-08"),
        Method = HttpMethod.Post,
        Content = new StringContent(requestBodyJson, Encoding.UTF8,"application/json")
    };
    changeWriteLocationRequest.Headers.Add("Authorization", "Bearer " + token);

    var changeWriteLocationResp = await httpClient.SendAsync(changeWriteLocationRequest);
    log.LogInformation("Response status: " + changeWriteLocationResp.StatusCode);
    changeWriteLocationResp.EnsureSuccessStatusCode();
}
