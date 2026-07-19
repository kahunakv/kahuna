namespace RadLine
{
    public sealed class BackspaceCommand : LineEditorCommand
    {
        public override void Execute(LineEditorContext context)
        {
            // At the very beginning of a line, backspace joins this line onto the
            // previous one. The editor decides whether that's actually possible
            // (multiline, and not already the first line); here we signal the intent.
            if (context.Buffer.AtBeginning)
            {
                context.Submit(SubmitAction.Backspace);
                return;
            }

            var removed = context.Buffer.Clear(context.Buffer.Position - 1, 1);
            if (removed == 1)
            {
                context.Buffer.Move(context.Buffer.Position - 1);
            }
        }
    }
}
