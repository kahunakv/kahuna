
using Kommander;
using Timer = System.Threading.Timer;

namespace Kahuna.Services;

public sealed class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IKahuna kahuna;
    
    private readonly IRaft raft;
    
    private readonly ILogger<IRaft> logger;

    private readonly ThreadStatsLogger x;

    public ReplicationService(IKahuna kahuna, IRaft raft, ILogger<IRaft> logger)
    {
        this.kahuna = kahuna;
        this.raft = raft;
        this.logger = logger;
        this.x = new(logger);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (raft.Configuration.Host == "*")
            return;
        
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        
        await raft.JoinCluster();
        
        while (true)
        {
            await raft.UpdateNodes();

            await Task.Delay(3000, stoppingToken);
        }
    }
}

public sealed class ThreadStatsLogger : IDisposable
{
    private const int DEPLETION_WARN_LEVEL = 10;
    
    private const int HISTERESIS_LEVEL = 10;

    private const double SAMPLE_RATE_MILLISECONDS = 30000;
    
    private bool _workerThreadWarned = false;
    
    private bool _ioThreadWarned = false;
    
    private bool _minWorkerThreadLevelWarned = false;
    
    private bool _minIoThreadLevelWarned = false;

    private readonly int _maxWorkerThreadLevel;
    
    private readonly int _maxIoThreadLevel;
    
    private readonly int _minWorkerThreadLevel;
    
    private readonly int _minWorkerThreadLevelRecovery;
    
    private readonly int _minIoThreadLevel;
    
    private readonly int _minIoThreadLevelRecovery;
    
    private readonly Timer _timer;
    
    private readonly ILogger<IRaft> _logger;

    public ThreadStatsLogger(ILogger<IRaft> logger)
    {
        _logger = logger;
        
        _timer = new(TimerElasped, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(SAMPLE_RATE_MILLISECONDS));
        
        ThreadPool.GetMinThreads(out _minWorkerThreadLevel, out _minIoThreadLevel);
        ThreadPool.GetMaxThreads(out _maxWorkerThreadLevel, out _maxIoThreadLevel);
        ThreadPool.GetAvailableThreads(out int workerAvailable, out int ioAvailable);

        logger.LogInformation("Thread statistics at startup: minimum worker:{Workers} io:{IO}", _minWorkerThreadLevel, _minIoThreadLevel);
        logger.LogInformation("Thread statistics at startup: maximum worker:{Workers} io:{IO}", _maxWorkerThreadLevel, _maxIoThreadLevel);
        logger.LogInformation("Thread statistics at startup: available worker:{Workers} io:{IO}", workerAvailable, ioAvailable);

        _minWorkerThreadLevelRecovery = (_minWorkerThreadLevel * 3) / 4;
        _minIoThreadLevelRecovery = (_minIoThreadLevel * 3) / 4;
        
        if (_minWorkerThreadLevelRecovery == _minWorkerThreadLevel)
            _minWorkerThreadLevelRecovery = _minWorkerThreadLevel - 1;
        
        if (_minIoThreadLevelRecovery == _minIoThreadLevel) 
            _minIoThreadLevelRecovery = _minIoThreadLevel - 1;
    }

    private void TimerElasped(object? sender)
    {
        ThreadPool.GetAvailableThreads(out int availableWorkerThreads, out int availableIoThreads);

        int activeWorkerThreads = _maxWorkerThreadLevel - availableWorkerThreads;
        int activeIoThreads = _maxIoThreadLevel - availableIoThreads;

        _logger.LogInformation("Thread statistics: active worker:{Wokers} io:{Io}", activeWorkerThreads, activeIoThreads);

        if (activeWorkerThreads > _minWorkerThreadLevel && !_minWorkerThreadLevelWarned)
        {
            _logger.LogWarning("Thread statistics WARN active worker threads above minimum {Workers}:{Io}", activeWorkerThreads, _minWorkerThreadLevel);
            
            _minWorkerThreadLevelWarned = !_minWorkerThreadLevelWarned;
        }

        if (activeWorkerThreads < _minWorkerThreadLevelRecovery && _minWorkerThreadLevelWarned)
        {
            _logger.LogWarning("Thread statistics RECOVERY active worker threads below minimum {Workers}:{Io}", activeWorkerThreads, _minWorkerThreadLevel);
            
            _minWorkerThreadLevelWarned = !_minWorkerThreadLevelWarned;
        }

        if (activeIoThreads > _minIoThreadLevel && !_minIoThreadLevelWarned)
        {
            _logger.LogWarning("Thread statistics WARN active io threads above minimum {Workers}:{Io}", activeIoThreads, _minIoThreadLevel);
            
            _minIoThreadLevelWarned = !_minIoThreadLevelWarned;
        }

        if (activeIoThreads < _minIoThreadLevelRecovery && _minIoThreadLevelWarned)
        {
            _logger.LogWarning("Thread statistics RECOVERY active io threads below minimum {Workers}:{Io}", activeIoThreads, _minIoThreadLevel);
            
            _minIoThreadLevelWarned = !_minIoThreadLevelWarned;
        }

        if (availableWorkerThreads < DEPLETION_WARN_LEVEL && !_workerThreadWarned)
        {
            _logger.LogWarning("Thread statistics WARN available worker threads below warning level {Workers}:{Io}", availableWorkerThreads, DEPLETION_WARN_LEVEL);
            
            _workerThreadWarned = !_workerThreadWarned;
        }

        if (availableWorkerThreads > (DEPLETION_WARN_LEVEL + HISTERESIS_LEVEL) && _workerThreadWarned)
        {
            _logger.LogWarning("Thread statistics RECOVERY available worker thread recovery {Workers}:{Io}", availableWorkerThreads, DEPLETION_WARN_LEVEL);
            
            _workerThreadWarned = !_workerThreadWarned;
        }

        if (availableIoThreads < DEPLETION_WARN_LEVEL && !_ioThreadWarned)
        {
            _logger.LogWarning("Thread statistics WARN available io threads below warning level {Workers}:{Io}", availableIoThreads, DEPLETION_WARN_LEVEL);
            
            _ioThreadWarned = !_ioThreadWarned;
        }

        if (availableIoThreads > (DEPLETION_WARN_LEVEL + HISTERESIS_LEVEL) && _ioThreadWarned)
        {
            _logger.LogWarning("Thread statistics RECOVERY available io thread recovery {Workers}:{Io}", availableIoThreads, DEPLETION_WARN_LEVEL);
            
            _ioThreadWarned = !_ioThreadWarned;
        }
    }

    public void Dispose()
    {
        _timer.Dispose();
    }
}