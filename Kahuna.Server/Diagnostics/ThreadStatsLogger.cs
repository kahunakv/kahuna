
using Kommander;

namespace Kahuna.Server.Diagnostics;

public sealed class ThreadStatsLogger : IDisposable
{
    private const int DEPLETION_WARN_LEVEL = 10;
    
    private const int HISTERESIS_LEVEL = 10;

    private const double SAMPLE_RATE_MILLISECONDS = 30000;
    
    private bool _workerThreadWarned;
    
    private bool _ioThreadWarned;
    
    private bool _minWorkerThreadLevelWarned;
    
    private bool _minIoThreadLevelWarned;

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

        _logger.LogTrace("Thread statistics: active worker:{Wokers} io:{Io}", activeWorkerThreads, activeIoThreads);

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