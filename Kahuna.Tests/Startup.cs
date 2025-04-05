
using Microsoft.Extensions.DependencyInjection;

namespace Kahuna.Tests;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<IKahuna, KahunaManager>();
    }
}