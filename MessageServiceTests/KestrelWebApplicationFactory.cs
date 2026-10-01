using System.Collections.Generic;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

public class KestrelWebApplicationFactory<TProgram> : WebApplicationFactory<TProgram> where TProgram : class
{
    private readonly int _port;
    private readonly Dictionary<string, string> _configOverrides;
    public IHost? RealKestrelHost { get; private set; }
    
    public KestrelWebApplicationFactory(int port, Dictionary<string, string>? configOverrides = null)
    {
        _port = port;
        _configOverrides = configOverrides ?? new Dictionary<string, string>();
    }

    protected override IHost CreateHost(IHostBuilder builder)
    {
        builder.ConfigureWebHost(webBuilder =>
        {
            webBuilder.UseKestrel(options =>
            {
                options.ListenLocalhost(_port, o => o.Protocols = HttpProtocols.Http2);
            });
            if (_configOverrides.Count > 0)
            {
                webBuilder.ConfigureAppConfiguration((context, config) =>
                {
                    config.AddInMemoryCollection(_configOverrides);
                });
            }
        });
        RealKestrelHost = builder.Build();
        RealKestrelHost.Start();

        var dummyHost = new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.Configure(app => 
                {
                    app.Run(context => Task.CompletedTask); 
                });
            })
            .Build();
        
        dummyHost.Start();
        
        return dummyHost;
    }
    
    public void Boot()
    {
        _ = CreateClient(); 
    }
}