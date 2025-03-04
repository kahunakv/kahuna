namespace Kahuna.Utils;

public static class PathUtils
{
    public static void CreateDirectoryIfNotExists(string path)
    {
        if (!Directory.Exists(path))
            Directory.CreateDirectory(path);
    }

    private static void CreateDirectoryRecursive(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
        {
            CreateDirectoryRecursive(Path.GetDirectoryName(directoryPath)!);
            CreateDirectoryIfNotExists(directoryPath);
        }
    }
}