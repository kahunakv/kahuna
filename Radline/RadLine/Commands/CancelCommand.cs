namespace RadLine
{
    public sealed class CancelCommand : LineEditorCommand
    {
        public override void Execute(LineEditorContext context)
        {
            context.Submit(SubmitAction.Cancel);
        }
    }
}
