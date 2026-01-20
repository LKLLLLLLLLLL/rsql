<template>
  <div ref="editorRef" class="sql-editor"></div>
</template>

<script setup>
import { ref, onMounted, watch, onBeforeUnmount } from 'vue'
import { EditorView, lineNumbers, highlightActiveLineGutter, highlightSpecialChars, drawSelection, dropCursor, rectangularSelection, crosshairCursor, highlightActiveLine, keymap } from '@codemirror/view'
import { sql } from '@codemirror/lang-sql'
import { EditorState } from '@codemirror/state'
import { history } from '@codemirror/commands'
import { defaultKeymap, historyKeymap, indentWithTab } from '@codemirror/commands'
import { searchKeymap } from '@codemirror/search'
import { autocompletion, completionKeymap, closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete'
import { foldGutter, foldKeymap, bracketMatching, indentOnInput, syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language'
import { lintKeymap } from '@codemirror/lint'
import { oneDark } from '@codemirror/theme-one-dark'

const props = defineProps({
  modelValue: {
    type: String,
    default: ''
  },
  disabled: {
    type: Boolean,
    default: false
  },
  placeholder: {
    type: String,
    default: 'Enter SQL...'
  }
})

const emit = defineEmits(['update:modelValue'])

const editorRef = ref(null)
let editorView = null

onMounted(() => {
  const extensions = [
    lineNumbers(),
    highlightActiveLineGutter(),
    highlightSpecialChars(),
    history(),
    foldGutter(),
    drawSelection(),
    dropCursor(),
    EditorState.allowMultipleSelections.of(true),
    indentOnInput(),
    syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
    bracketMatching(),
    closeBrackets(),
    autocompletion(),
    rectangularSelection(),
    crosshairCursor(),
    highlightActiveLine(),
    // 移除 highlightSelectionMatches() - 这会高亮相同的词
    keymap.of([
      ...closeBracketsKeymap,
      ...defaultKeymap,
      ...searchKeymap,
      ...historyKeymap,
      ...foldKeymap,
      ...completionKeymap,
      ...lintKeymap,
      indentWithTab
    ]),
    sql(),
    oneDark,
    EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        emit('update:modelValue', update.state.doc.toString())
      }
    }),
    EditorView.editable.of(!props.disabled)  // 设置初始可编辑状态
  ]

  editorView = new EditorView({
    state: EditorState.create({
      doc: props.modelValue,
      extensions
    }),
    parent: editorRef.value
  })
})

watch(
  () => props.modelValue,
  (newValue) => {
    if (editorView && newValue !== editorView.state.doc.toString()) {
      editorView.dispatch({
        changes: {
          from: 0,
          to: editorView.state.doc.length,
          insert: newValue
        }
      })
    }
  }
)

watch(
  () => props.disabled,
  (newDisabled) => {
    if (editorView) {
      editorView.contentDOM.contentEditable = !newDisabled
      // 更新编辑器状态
      const view = editorView
      view.dispatch({})
    }
  }
)

onBeforeUnmount(() => {
  if (editorView) {
    editorView.destroy()
  }
})
</script>

<style scoped>
.sql-editor {
  width: 100%;
  height: 100%;
  overflow: hidden;
  border-radius: 4px;
}

.sql-editor :deep(.cm-editor) {
  height: 100%;
  font-size: 18px;
  font-family: 'Consolas', 'Courier New', monospace;
}

.sql-editor :deep(.cm-content) {
  font-size: 18px;
}

.sql-editor :deep(.cm-line) {
  padding: 2px 0;
}
</style>